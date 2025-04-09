use crossterm::{
    event::{self, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, Paragraph, Row, Table, TableState, Tabs},
    Terminal,
};
use std::io;
use crate::{Dataset, Description, PrestoError};
use serde_json;

pub fn render_tui(dataset: &Dataset, description: &Description) -> Result<(), PrestoError> {
    enable_raw_mode().map_err(|e| PrestoError::InvalidNumeric(e.to_string()))?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen).map_err(|e| PrestoError::InvalidNumeric(e.to_string()))?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend).map_err(|e| PrestoError::InvalidNumeric(e.to_string()))?;
    let mut tab_index = 0;
    let mut table_state = TableState::default();
    let mut table_h_scroll = 0usize;
    let mut corr_state = TableState::default();
    let mut corr_h_scroll = 0usize;
    let mut details_v_scroll = 0u16;
    let mut details_h_scroll = 0u16;
    let mut advanced_v_scroll = 0u16;
    let mut advanced_h_scroll = 0u16;
    let mut plots_v_scroll = 0u16;
    let mut plots_h_scroll = 0u16;

    loop {
        let size = terminal.size().map_err(|e| PrestoError::InvalidNumeric(e.to_string()))?;
        let full_area = Rect::new(0, 0, size.width, size.height);
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .margin(1)
            .constraints([
                Constraint::Length(3),  
                Constraint::Length(3),  
                Constraint::Min(10),    
                Constraint::Length(3),  
            ])
            .split(full_area);
        let content_area = chunks[2];
        let content_height = content_area.height.saturating_sub(2) as usize;
        let content_width = content_area.width.saturating_sub(2) as usize;

        let header_cells = vec![
            "Column", "Mean", "Median", "StdDev", "Variance", "Min", "Max", "Skew", "Kurt",
        ];
        let widths = [15usize, 10, 10, 10, 10, 10, 10, 10, 10];
        let total_cols = header_cells.len();
        let total_width: usize = widths.iter().sum();

        terminal.draw(|f| {
            let title = Paragraph::new("⚡ Presto Presto accelerates preprocessing with precision ⚡")
                .style(Style::default().fg(Color::Green).add_modifier(Modifier::BOLD))
                .block(Block::default().borders(Borders::ALL).border_style(Style::default().fg(Color::Cyan)));
            f.render_widget(title, chunks[0]);

            let tab_titles = vec!["📊 Stats", "📋 Details", "🔍 Advanced", "🔗 Correlations", "📈 Plots"];
            let tabs = Tabs::new(tab_titles.into_iter().map(String::from).collect::<Vec<_>>())
                .select(tab_index)
                .style(Style::default().fg(Color::White))
                .highlight_style(Style::default().fg(Color::Green).add_modifier(Modifier::BOLD))
                .divider("│");
            f.render_widget(tabs, chunks[1]);

            match tab_index {
                0 => { 
                    let mut visible_width = 0;
                    let mut end_col = table_h_scroll;
                    for i in table_h_scroll..total_cols {
                        visible_width += widths[i];
                        if visible_width > content_width {
                            end_col = i;
                            break;
                        }
                        end_col = i + 1;
                    }
                    let start_col = table_h_scroll;
                    let visible_headers = &header_cells[start_col..end_col];
                    let visible_widths = &widths[start_col..end_col];

                    let all_rows: Vec<Row> = dataset.headers.iter().enumerate().map(|(i, header)| {
                        let stats = &description.stats[i];
                        let skew_desc = stats.skewness.map(|s| match s {
                            s if s > 1.0 => "Highly +ve skewed",
                            s if s > 0.5 => "Mod. +ve skewed",
                            s if s < -1.0 => "Highly -ve skewed",
                            s if s < -0.5 => "Mod. -ve skewed",
                            _ => "Symmetric",
                        }).unwrap_or("N/A");
                        let kurt_desc = stats.kurtosis.map(|k| match k {
                            k if k > 3.0 => "Leptokurtic",
                            k if k < 3.0 => "Platykurtic",
                            _ => "Mesokurtic",
                        }).unwrap_or("N/A");
                        Row::new(vec![
                            header.clone(),
                            stats.mean.map_or("N/A".to_string(), |v| format!("{:.2}", v)),
                            stats.median.map_or("N/A".to_string(), |v| format!("{:.2}", v)),
                            stats.std_dev.map_or("N/A".to_string(), |v| format!("{:.2}", v)),
                            stats.variance.map_or("N/A".to_string(), |v| format!("{:.2}", v)),
                            stats.min.map_or("N/A".to_string(), |v| format!("{:.2}", v)),
                            stats.max.map_or("N/A".to_string(), |v| format!("{:.2}", v)),
                            stats.skewness.map_or("N/A".to_string(), |v| format!("{:.2} ({})", v, skew_desc)),
                            stats.kurtosis.map_or("N/A".to_string(), |v| format!("{:.2} ({})", v, kurt_desc)),
                        ][start_col..end_col].to_vec())
                    }).collect();

                    let header = Row::new(visible_headers.to_vec()).style(Style::default().fg(Color::Green));
                    let stats_table = Table::new(all_rows, visible_widths.iter().map(|&w| Constraint::Length(w as u16)))
                        .header(header)
                        .block(Block::default()
                            .title("Statistics")
                            .borders(Borders::ALL)
                            .border_type(BorderType::Thick)
                            .border_style(Style::default().fg(Color::Cyan)))
                        .column_spacing(1)
                        .style(Style::default().fg(Color::White));
                    if dataset.headers.len() > content_height {
                        f.render_stateful_widget(stats_table, content_area, &mut table_state);
                    } else {
                        f.render_widget(stats_table, content_area);
                    }
                }
                1 => { 
                    let info_text: Vec<Line> = vec![
                        Line::from(vec![Span::styled("Rows: ", Style::default().fg(Color::Magenta)), Span::raw(description.total_rows.to_string())]),
                        Line::from(vec![Span::styled("Cols: ", Style::default().fg(Color::Magenta)), Span::raw(dataset.headers.len().to_string())]),
                        Line::from(vec![Span::styled("Missing %: ", Style::default().fg(Color::Magenta)), Span::raw(format!("{:.1}", description.missing_pct))]),
                        Line::from(vec![Span::styled("Unique %: ", Style::default().fg(Color::Magenta)), Span::raw(format!("{:.1}", description.unique_pct))]),
                        Line::from(vec![Span::styled("Missing: ", Style::default().fg(Color::Magenta)), Span::raw(description.missing.iter().map(|&m| m.to_string()).collect::<Vec<_>>().join(", "))]),
                        Line::from(vec![Span::styled("Duplicates: ", Style::default().fg(Color::Magenta)), Span::raw(description.duplicates.to_string())]),
                        Line::from(vec![Span::styled("Outliers: ", Style::default().fg(Color::Magenta)), Span::raw(description.outliers.iter().enumerate().map(|(i, o)| format!("{}: {:?}", dataset.headers[i], o)).collect::<Vec<_>>().join(", "))]),
                        Line::from(vec![Span::styled("Types: ", Style::default().fg(Color::Magenta)), Span::raw(description.types.iter().map(|t| format!("{:?}", t)).collect::<Vec<_>>().join(", "))]),
                        Line::from(vec![Span::styled("Cardinality: ", Style::default().fg(Color::Blue)), Span::raw(description.cardinality.iter().map(|&c| c.to_string()).collect::<Vec<_>>().join(", "))]),
                        Line::from(vec![Span::styled("Distributions: ", Style::default().fg(Color::Blue)), Span::raw(description.distributions.iter().map(|d| d.iter().map(|&(mid, cnt)| format!("{:.1}:{}", mid, cnt)).collect::<Vec<_>>().join("|")).collect::<Vec<_>>().join(", "))]),
                        Line::from(vec![Span::styled("Top Values: ", Style::default().fg(Color::Blue)), Span::raw(description.top_values.iter().map(|(col, vals)| format!("{}: {}", col, vals.iter().map(|(v, c)| format!("{}({})", v, c)).collect::<Vec<_>>().join(", "))).collect::<Vec<_>>().join("; "))]),
                    ];
                    let info_block = Paragraph::new(info_text.clone())
                        .block(Block::default()
                            .title("Details")
                            .borders(Borders::ALL)
                            .border_type(BorderType::Thick)
                            .border_style(Style::default().fg(Color::Cyan)))
                        .style(Style::default().fg(Color::White))
                        .scroll((details_v_scroll, details_h_scroll));
                    f.render_widget(info_block, content_area);
                }
                2 => { 
                    let advanced_text: Vec<Line> = vec![
                        Line::from(vec![Span::styled("Dependency: ", Style::default().fg(Color::Green)), Span::raw(description.dependency_scores.iter().map(|&s| format!("{:.2}", s)).collect::<Vec<_>>().join(", "))]),
                        Line::from(vec![Span::styled("Drift: ", Style::default().fg(Color::Green)), Span::raw(description.drift_scores.iter().map(|&s| format!("{:.2}", s)).collect::<Vec<_>>().join(", "))]),
                        Line::from(vec![Span::styled("Consistency Issues: ", Style::default().fg(Color::Red)), Span::raw(description.consistency_issues.iter().map(|&i| i.to_string()).collect::<Vec<_>>().join(", "))]),
                        Line::from(vec![Span::styled("Temporal: ", Style::default().fg(Color::Red)), Span::raw(description.temporal_patterns.join(", "))]),
                        Line::from(vec![Span::styled("Transforms: ", Style::default().fg(Color::Red)), Span::raw(description.transform_suggestions.join(", "))]),
                        Line::from(vec![Span::styled("Noise: ", Style::default().fg(Color::Yellow)), Span::raw(description.noise_scores.iter().map(|&n| format!("{:.2}", n)).collect::<Vec<_>>().join(", "))]),
                        Line::from(vec![Span::styled("Redundancy: ", Style::default().fg(Color::Yellow)), Span::raw(
                            if description.redundancy_pairs.is_empty() {
                                "None".to_string()
                            } else {
                                description.redundancy_pairs.iter()
                                    .map(|&(i, j, s)| format!("{}<->{}:{:.2}", dataset.headers[i], dataset.headers[j], s))
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            }
                        )]),
                        Line::from(vec![Span::styled("Feature Importance: ", Style::default().fg(Color::Green)), Span::raw(description.feature_importance.iter().map(|&(col, score)| format!("{}:{:.2}", dataset.headers[col], score)).collect::<Vec<_>>().join(", "))]),
                        Line::from(vec![Span::styled("Anomalies: ", Style::default().fg(Color::Red)), Span::raw(description.anomalies.iter().map(|(col, val, idx)| format!("{}:{} (idx {})", dataset.headers[*col], val, idx)).collect::<Vec<_>>().join(", "))]),
                    ];
                    let advanced_block = Paragraph::new(advanced_text.clone())
                        .block(Block::default()
                            .title("Advanced")
                            .borders(Borders::ALL)
                            .border_type(BorderType::Thick)
                            .border_style(Style::default().fg(Color::Cyan)))
                        .style(Style::default().fg(Color::White))
                        .scroll((advanced_v_scroll, advanced_h_scroll));
                    f.render_widget(advanced_block, content_area);
                }
                3 => { 
                    let corr_headers = dataset.headers.clone();
                    let corr_widths = vec![15usize; corr_headers.len() + 1];
                    let total_corr_cols = corr_headers.len() + 1;
                    let _total_corr_width: usize = corr_widths.iter().sum();

                    let mut visible_width = 0;
                    let mut end_col = corr_h_scroll;
                    for i in corr_h_scroll..total_corr_cols {
                        visible_width += corr_widths[i];
                        if visible_width > content_width {
                            end_col = i;
                            break;
                        }
                        end_col = i + 1;
                    }
                    let start_col = corr_h_scroll;
                    let visible_headers = &corr_headers[start_col.saturating_sub(1)..end_col.saturating_sub(1)];

                    let all_rows: Vec<Row> = dataset.headers.iter().enumerate().map(|(i, header)| {
                        let mut row = vec![header.clone()];
                        row.extend(description.correlations[i].iter().map(|&c| format!("{:.2}", c)));
                        Row::new(row[start_col..end_col].to_vec())
                    }).collect();

                    let header = Row::new(["".to_string()].iter().chain(visible_headers).cloned().collect::<Vec<_>>()).style(Style::default().fg(Color::Green));
                    let corr_table = Table::new(all_rows, corr_widths[start_col..end_col].iter().map(|&w| Constraint::Length(w as u16)))
                        .header(header)
                        .block(Block::default()
                            .title("Correlations")
                            .borders(Borders::ALL)
                            .border_type(BorderType::Thick)
                            .border_style(Style::default().fg(Color::Cyan)))
                        .column_spacing(1)
                        .style(Style::default().fg(Color::White));
                    if dataset.headers.len() > content_height {
                        f.render_stateful_widget(corr_table, content_area, &mut corr_state);
                    } else {
                        f.render_widget(corr_table, content_area);
                    }
                }
                4 => { 
                    let mut plot_text: Vec<Line> = Vec::new();
                    let max_height = content_area.height.saturating_sub(4) as usize;
                    for (i, header) in dataset.headers.iter().enumerate() {
                        plot_text.push(Line::from(Span::styled(format!("{}:", header), Style::default().fg(Color::Blue).add_modifier(Modifier::BOLD))));
                        if let Some(dist) = description.distributions.get(i) {
                            if dist.is_empty() {
                                plot_text.push(Line::from(Span::raw("  (No numeric data)")));
                                continue;
                            }
                            let max_val = dist.iter().map(|&(_, c)| c).max().unwrap_or(1) as f64;
                            let bar_heights: Vec<usize> = dist.iter()
                                .map(|&(_, cnt)| (cnt as f64 / max_val * max_height as f64).round() as usize)
                                .collect();
                            let max_label_width = dist.iter()
                                .map(|&(mid, _)| format!("{:.1}", mid).len())
                                .max()
                                .unwrap_or(4);
                            let step = max_val / max_height as f64;
                            for h in (0..=max_height).rev() {
                                let count = (h as f64 * step).round() as usize;
                                let mut line = format!("{:4} | ", count);
                                for (j, &height) in bar_heights.iter().enumerate() {
                                    let mid_str = format!("{:.1}", dist[j].0);
                                    let padding = max_label_width.saturating_sub(mid_str.len()) / 2;
                                    if h == 0 {
                                        line.push_str(&" ".repeat(padding));
                                        line.push_str(&mid_str);
                                        line.push_str(&" ".repeat(max_label_width.saturating_sub(mid_str.len() - padding)));
                                    } else {
                                        line.push_str(&" ".repeat(max_label_width / 2));
                                        line.push(if height >= h { '█' } else { ' ' });
                                        line.push_str(&" ".repeat(max_label_width / 2));
                                    }
                                    line.push(' ');
                                }
                                plot_text.push(Line::from(Span::raw(line)));
                            }
                        }
                        plot_text.push(Line::from(Span::raw(""))); 
                    }
                    let plot_block = Paragraph::new(plot_text.clone())
                        .block(Block::default()
                            .title("Plots")
                            .borders(Borders::ALL)
                            .border_type(BorderType::Thick)
                            .border_style(Style::default().fg(Color::Cyan)))
                        .style(Style::default().fg(Color::White))
                        .scroll((plots_v_scroll, plots_h_scroll));
                    f.render_widget(plot_block, content_area);
                }
                _ => unreachable!(),
            }

            let footer = Paragraph::new("'q' to exit | 'e' to export | Tab/Shift+Tab to switch tabs")
                .style(Style::default().fg(Color::Gray))
                .block(Block::default().borders(Borders::ALL).border_style(Style::default().fg(Color::Cyan)));
            f.render_widget(footer, chunks[3]);
        }).map_err(|e| PrestoError::InvalidNumeric(e.to_string()))?;

        if let Event::Key(key) = event::read().map_err(|e| PrestoError::InvalidNumeric(e.to_string()))? {
            match key.code {
                KeyCode::Char('q') => break,
                KeyCode::Char('e') => {
                    let json = serde_json::to_string_pretty(&description)
                        .map_err(|e| PrestoError::InvalidNumeric(e.to_string()))?;
                    std::fs::write("presto_insights.json", json)
                        .map_err(|e| PrestoError::InvalidNumeric(e.to_string()))?;
                }
                KeyCode::Tab => tab_index = (tab_index + 1) % 5,
                KeyCode::BackTab => tab_index = (tab_index + 4) % 5,
                KeyCode::Left => {
                    match tab_index {
                        0 => if total_width > content_width && table_h_scroll > 0 { table_h_scroll -= 1; }
                        1 => {
                            let info_text = vec![
                                format!("Rows: {}", description.total_rows),
                                format!("Cols: {}", dataset.headers.len()),
                                format!("Missing %: {:.1}", description.missing_pct),
                                format!("Unique %: {:.1}", description.unique_pct),
                                format!("Missing: {}", description.missing.iter().map(|&m| m.to_string()).collect::<Vec<_>>().join(", ")),
                                format!("Duplicates: {}", description.duplicates),
                                format!("Outliers: {}", description.outliers.iter().enumerate().map(|(i, o)| format!("{}: {:?}", dataset.headers[i], o)).collect::<Vec<_>>().join(", ")),
                                format!("Types: {}", description.types.iter().map(|t| format!("{:?}", t)).collect::<Vec<_>>().join(", ")),
                                format!("Cardinality: {}", description.cardinality.iter().map(|&c| c.to_string()).collect::<Vec<_>>().join(", ")),
                                format!("Distributions: {}", description.distributions.iter().map(|d| d.iter().map(|&(mid, cnt)| format!("{:.1}:{}", mid, cnt)).collect::<Vec<_>>().join("|")).collect::<Vec<_>>().join(", ")),
                                format!("Top Values: {}", description.top_values.iter().map(|(col, vals)| format!("{}: {}", col, vals.iter().map(|(v, c)| format!("{}({})", v, c)).collect::<Vec<_>>().join(", "))).collect::<Vec<_>>().join("; ")),
                            ];
                            let max_line_width = info_text.iter().map(|s| s.len()).max().unwrap_or(0);
                            if max_line_width > content_width && details_h_scroll > 0 { details_h_scroll -= 1; }
                        }
                        2 => {
                            let advanced_text = vec![
                                format!("Dependency: {}", description.dependency_scores.iter().map(|&s| format!("{:.2}", s)).collect::<Vec<_>>().join(", ")),
                                format!("Drift: {}", description.drift_scores.iter().map(|&s| format!("{:.2}", s)).collect::<Vec<_>>().join(", ")),
                                format!("Consistency Issues: {}", description.consistency_issues.iter().map(|&i| i.to_string()).collect::<Vec<_>>().join(", ")),
                                format!("Temporal: {}", description.temporal_patterns.join(", ")),
                                format!("Transforms: {}", description.transform_suggestions.join(", ")),
                                format!("Noise: {}", description.noise_scores.iter().map(|&n| format!("{:.2}", n)).collect::<Vec<_>>().join(", ")),
                                format!("Redundancy: {}", if description.redundancy_pairs.is_empty() {
                                    "None".to_string()
                                } else {
                                    description.redundancy_pairs.iter()
                                        .map(|&(i, j, s)| format!("{}<->{}:{:.2}", dataset.headers[i], dataset.headers[j], s))
                                        .collect::<Vec<_>>()
                                        .join(", ")
                                }),
                                format!("Feature Importance: {}", description.feature_importance.iter().map(|&(col, score)| format!("{}:{:.2}", dataset.headers[col], score)).collect::<Vec<_>>().join(", ")),
                                format!("Anomalies: {}", description.anomalies.iter().map(|(col, val, idx)| format!("{}:{} (idx {})", dataset.headers[*col], val, idx)).collect::<Vec<_>>().join(", ")),
                            ];
                            let max_line_width = advanced_text.iter().map(|s| s.len()).max().unwrap_or(0);
                            if max_line_width > content_width && advanced_h_scroll > 0 { advanced_h_scroll -= 1; }
                        }
                        3 => {
                            let corr_widths = vec![15usize; dataset.headers.len() + 1];
                            let total_corr_width: usize = corr_widths.iter().sum();
                            if total_corr_width > content_width && corr_h_scroll > 0 { corr_h_scroll -= 1; }
                        }
                        4 => {
                            let mut plot_text = Vec::new();
                            let max_height = content_area.height.saturating_sub(4) as usize;
                            let mut max_label_width = 4;
                            for (i, header) in dataset.headers.iter().enumerate() {
                                plot_text.push(format!("{}:", header));
                                if let Some(dist) = description.distributions.get(i) {
                                    if dist.is_empty() {
                                        plot_text.push("  (No numeric data)".to_string());
                                        continue;
                                    }
                                    max_label_width = dist.iter()
                                        .map(|&(mid, _)| format!("{:.1}", mid).len())
                                        .max()
                                        .unwrap_or(4)
                                        .max(max_label_width);
                                    let max_val = dist.iter().map(|&(_, c)| c).max().unwrap_or(1) as f64;
                                    let bar_heights: Vec<usize> = dist.iter()
                                        .map(|&(_, cnt)| (cnt as f64 / max_val * max_height as f64).round() as usize)
                                        .collect();
                                    let step = max_val / max_height as f64;
                                    for h in (0..=max_height).rev() {
                                        let count = (h as f64 * step).round() as usize;
                                        let mut line = format!("{:4} | ", count);
                                        for (j, &height) in bar_heights.iter().enumerate() {
                                            let mid_str = format!("{:.1}", dist[j].0);
                                            let padding = max_label_width.saturating_sub(mid_str.len()) / 2;
                                            if h == 0 {
                                                line.push_str(&" ".repeat(padding));
                                                line.push_str(&mid_str);
                                                line.push_str(&" ".repeat(max_label_width.saturating_sub(mid_str.len() - padding)));
                                            } else {
                                                line.push_str(&" ".repeat(max_label_width / 2));
                                                line.push(if height >= h { '█' } else { ' ' });
                                                line.push_str(&" ".repeat(max_label_width / 2));
                                            }
                                            line.push(' ');
                                        }
                                        plot_text.push(line);
                                    }
                                }
                                plot_text.push("".to_string());
                            }
                            let max_line_width = plot_text.iter().map(|s| s.len()).max().unwrap_or(0);
                            if max_line_width > content_width && plots_h_scroll > 0 { plots_h_scroll -= 1; }
                        }
                        _ => {}
                    }
                }
                KeyCode::Right => {
                    match tab_index {
                        0 => {
                            let mut visible_width = 0;
                            for &w in &widths[table_h_scroll..] {
                                if visible_width + w > content_width { break; }
                                visible_width += w;
                            }
                            let max_h_scroll = total_cols.saturating_sub((content_width / 10).max(1));
                            if total_width > content_width && table_h_scroll < max_h_scroll { table_h_scroll += 1; }
                        }
                        1 => {
                            let info_text = vec![
                                format!("Rows: {}", description.total_rows),
                                format!("Cols: {}", dataset.headers.len()),
                                format!("Missing %: {:.1}", description.missing_pct),
                                format!("Unique %: {:.1}", description.unique_pct),
                                format!("Missing: {}", description.missing.iter().map(|&m| m.to_string()).collect::<Vec<_>>().join(", ")),
                                format!("Duplicates: {}", description.duplicates),
                                format!("Outliers: {}", description.outliers.iter().enumerate().map(|(i, o)| format!("{}: {:?}", dataset.headers[i], o)).collect::<Vec<_>>().join(", ")),
                                format!("Types: {}", description.types.iter().map(|t| format!("{:?}", t)).collect::<Vec<_>>().join(", ")),
                                format!("Cardinality: {}", description.cardinality.iter().map(|&c| c.to_string()).collect::<Vec<_>>().join(", ")),
                                format!("Distributions: {}", description.distributions.iter().map(|d| d.iter().map(|&(mid, cnt)| format!("{:.1}:{}", mid, cnt)).collect::<Vec<_>>().join("|")).collect::<Vec<_>>().join(", ")),
                                format!("Top Values: {}", description.top_values.iter().map(|(col, vals)| format!("{}: {}", col, vals.iter().map(|(v, c)| format!("{}({})", v, c)).collect::<Vec<_>>().join(", "))).collect::<Vec<_>>().join("; ")),
                            ];
                            let max_line_width = info_text.iter().map(|s| s.len()).max().unwrap_or(0);
                            let max_h_scroll = max_line_width.saturating_sub(content_width) as u16;
                            if max_line_width > content_width && details_h_scroll < max_h_scroll { details_h_scroll += 1; }
                        }
                        2 => {
                            let advanced_text = vec![
                                format!("Dependency: {}", description.dependency_scores.iter().map(|&s| format!("{:.2}", s)).collect::<Vec<_>>().join(", ")),
                                format!("Drift: {}", description.drift_scores.iter().map(|&s| format!("{:.2}", s)).collect::<Vec<_>>().join(", ")),
                                format!("Consistency Issues: {}", description.consistency_issues.iter().map(|&i| i.to_string()).collect::<Vec<_>>().join(", ")),
                                format!("Temporal: {}", description.temporal_patterns.join(", ")),
                                format!("Transforms: {}", description.transform_suggestions.join(", ")),
                                format!("Noise: {}", description.noise_scores.iter().map(|&n| format!("{:.2}", n)).collect::<Vec<_>>().join(", ")),
                                format!("Redundancy: {}", if description.redundancy_pairs.is_empty() {
                                    "None".to_string()
                                } else {
                                    description.redundancy_pairs.iter()
                                        .map(|&(i, j, s)| format!("{}<->{}:{:.2}", dataset.headers[i], dataset.headers[j], s))
                                        .collect::<Vec<_>>()
                                        .join(", ")
                                }),
                                format!("Feature Importance: {}", description.feature_importance.iter().map(|&(col, score)| format!("{}:{:.2}", dataset.headers[col], score)).collect::<Vec<_>>().join(", ")),
                                format!("Anomalies: {}", description.anomalies.iter().map(|(col, val, idx)| format!("{}:{} (idx {})", dataset.headers[*col], val, idx)).collect::<Vec<_>>().join(", ")),
                            ];
                            let max_line_width = advanced_text.iter().map(|s| s.len()).max().unwrap_or(0);
                            let max_h_scroll = max_line_width.saturating_sub(content_width) as u16;
                            if max_line_width > content_width && advanced_h_scroll < max_h_scroll { advanced_h_scroll += 1; }
                        }
                        3 => {
                            let corr_widths = vec![15usize; dataset.headers.len() + 1];
                            let total_corr_width: usize = corr_widths.iter().sum();
                            let max_h_scroll = (dataset.headers.len() + 1).saturating_sub((content_width / 15).max(1));
                            if total_corr_width > content_width && corr_h_scroll < max_h_scroll { corr_h_scroll += 1; }
                        }
                        4 => {
                            let mut plot_text = Vec::new();
                            let max_height = content_area.height.saturating_sub(4) as usize;
                            let mut max_label_width = 4;
                            for (i, header) in dataset.headers.iter().enumerate() {
                                plot_text.push(format!("{}:", header));
                                if let Some(dist) = description.distributions.get(i) {
                                    if dist.is_empty() {
                                        plot_text.push("  (No numeric data)".to_string());
                                        continue;
                                    }
                                    max_label_width = dist.iter()
                                        .map(|&(mid, _)| format!("{:.1}", mid).len())
                                        .max()
                                        .unwrap_or(4)
                                        .max(max_label_width);
                                    let max_val = dist.iter().map(|&(_, c)| c).max().unwrap_or(1) as f64;
                                    let bar_heights: Vec<usize> = dist.iter()
                                        .map(|&(_, cnt)| (cnt as f64 / max_val * max_height as f64).round() as usize)
                                        .collect();
                                    let step = max_val / max_height as f64;
                                    for h in (0..=max_height).rev() {
                                        let count = (h as f64 * step).round() as usize;
                                        let mut line = format!("{:4} | ", count);
                                        for (j, &height) in bar_heights.iter().enumerate() {
                                            let mid_str = format!("{:.1}", dist[j].0);
                                            let padding = max_label_width.saturating_sub(mid_str.len()) / 2;
                                            if h == 0 {
                                                line.push_str(&" ".repeat(padding));
                                                line.push_str(&mid_str);
                                                line.push_str(&" ".repeat(max_label_width.saturating_sub(mid_str.len() - padding)));
                                            } else {
                                                line.push_str(&" ".repeat(max_label_width / 2));
                                                line.push(if height >= h { '█' } else { ' ' });
                                                line.push_str(&" ".repeat(max_label_width / 2));
                                            }
                                            line.push(' ');
                                        }
                                        plot_text.push(line);
                                    }
                                }
                                plot_text.push("".to_string());
                            }
                            let max_line_width = plot_text.iter().map(|s| s.len()).max().unwrap_or(0);
                            let max_h_scroll = max_line_width.saturating_sub(content_width) as u16;
                            if max_line_width > content_width && plots_h_scroll < max_h_scroll { plots_h_scroll += 1; }
                        }
                        _ => {}
                    }
                }
                KeyCode::Up => {
                    match tab_index {
                        0 => if dataset.headers.len() > content_height {
                            if let Some(selected) = table_state.selected() {
                                table_state.select(Some(selected.saturating_sub(1)));
                            } else {
                                table_state.select(Some(dataset.headers.len().saturating_sub(1)));
                            }
                        }
                        1 => {
                            let info_lines = 12usize;
                            if info_lines > content_height && details_v_scroll > 0 { details_v_scroll -= 1; }
                        }
                        2 => {
                            let advanced_lines = 9usize;
                            if advanced_lines > content_height && advanced_v_scroll > 0 { advanced_v_scroll -= 1; }
                        }
                        3 => if dataset.headers.len() > content_height {
                            if let Some(selected) = corr_state.selected() {
                                corr_state.select(Some(selected.saturating_sub(1)));
                            } else {
                                corr_state.select(Some(dataset.headers.len().saturating_sub(1)));
                            }
                        }
                        4 => {
                            let max_height = content_area.height.saturating_sub(4) as usize;
                            let plot_lines = dataset.headers.len() * (max_height + 2);
                            if plot_lines > content_height && plots_v_scroll > 0 { plots_v_scroll -= 1; }
                        }
                        _ => {}
                    }
                }
                KeyCode::Down => {
                    match tab_index {
                        0 => if dataset.headers.len() > content_height {
                            if let Some(selected) = table_state.selected() {
                                table_state.select(Some((selected + 1).min(dataset.headers.len() - 1)));
                            } else {
                                table_state.select(Some(0));
                            }
                        }
                        1 => {
                            let info_lines = 12usize;
                            let max_v_scroll = (info_lines.saturating_sub(content_height)) as u16;
                            if info_lines > content_height && details_v_scroll < max_v_scroll { details_v_scroll += 1; }
                        }
                        2 => {
                            let advanced_lines = 9usize;
                            let max_v_scroll = (advanced_lines.saturating_sub(content_height)) as u16;
                            if advanced_lines > content_height && advanced_v_scroll < max_v_scroll { advanced_v_scroll += 1; }
                        }
                        3 => if dataset.headers.len() > content_height {
                            if let Some(selected) = corr_state.selected() {
                                corr_state.select(Some((selected + 1).min(dataset.headers.len() - 1)));
                            } else {
                                corr_state.select(Some(0));
                            }
                        }
                        4 => {
                            let max_height = content_area.height.saturating_sub(4) as usize;
                            let plot_lines = dataset.headers.len() * (max_height + 2);
                            let max_v_scroll = (plot_lines.saturating_sub(content_height)) as u16;
                            if plot_lines > content_height && plots_v_scroll < max_v_scroll { plots_v_scroll += 1; }
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
        }
    }

    disable_raw_mode().map_err(|e| PrestoError::InvalidNumeric(e.to_string()))?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen).map_err(|e| PrestoError::InvalidNumeric(e.to_string()))?;
    terminal.show_cursor().map_err(|e| PrestoError::InvalidNumeric(e.to_string()))?;

    Ok(())
}
