# Presto
![Presto Snapshot](https://github.com/amirhosseinghanipour/presto/blob/main/examples/presto.png)

Presto is a Rust-based CLI tool for exploring and analyzing datasets through an interactive terminal user interface (TUI). It offers statistical insights, correlations, and ASCII visualizations for data analysts and developers who love the command line.

# Features
- Interactive TUI: Navigate tabs (📊 Stats, 📋 Details, 🔍 Advanced, 🔗 Correlations, 📈 Plots).
- Statistical Analysis: Means, medians, standard deviations, skewness, kurtosis, and more.
- Data Insights: Missing values, duplicates, outliers, and feature importance.
- Visualizations: ASCII bar plots for data distributions.
- Exportable Results: Save insights as JSON with the e key.
- Lightweight and Fast: Built in Rust for performance.

# Installation
## From crates.io
```bash
cargo install presto
```

## From GitHub Release
1. Download the latest release.
2. Extract the binary.
3. Move it to a directory in your PATH:
```bash
mv presto /usr/local/bin/
```

## From Source
1. Clone the repo:
```bash
git clone https://github.com/amirhosseinghanipour/presto.git
cd presto
```
2. Build and install:
```bash
cargo install --path .
```

# Usage
Run Presto with a CSV dataset:
```bash
presto -p <path_to_csv>
```

Example:
```bash
presto -p data.csv
```

## TUI Controls
- Tabs: Tab / Shift+Tab to switch sections.
- Navigation: ↑ / ↓ / ← / → to scroll content.
- Export: Press e to save insights as presto_insights.json.
- Exit: Press q to quit.

# Contributing
Contributions are always welcomed! To get started:
1. Fork the repository.
2. Create a branch: `git checkout -b feature-name`.
3. Commit your changes: `git commit -m "Add feature"`.
4. Push to your fork: `git push origin feature-name`.
5. Open a pull request.

## Ideas for Contributions
- Add support for more file formats (e.g., JSON, Parquet).
- Implement additional plot types (e.g., line charts).
- Add CLI flags for customization (e.g., output file path).

# License
Presto is licensed under MIT. See the [LICENSE](LICENSE) file for details.

# Acknowledgments
- Built with `Rust`, `ratatui`, `crossterm`, and `clap`.
- Inspired by terminal tools like `htop` and `ncdu`.
