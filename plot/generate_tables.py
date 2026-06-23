import os
import pandas as pd


def generate_tables():
    csv_path = '../data/benchmark_results.csv'
    output_dir = '../data/plots'

    if not os.path.exists(csv_path):
        print(f"Ошибка: Файл {csv_path} не найден")
        return

    df = pd.read_csv(csv_path)
    os.makedirs(output_dir, exist_ok=True)

    results = []

    for size in ['Small', 'Large']:
        for arch in ['Lambda', 'Kappa', 'Lakehouse']:
            subset = df[(df['DatasetSize'] == size) & (df['Architecture'] == arch)]
            if subset.empty:
                continue

            ingest_val = "N/A"
            if arch == 'Lambda':
                val = subset[subset['MetricName'] == 'BatchExecution']['ValueMs'].mean()
                ingest_val = f"{val / 1000.0:.2f} сек" if not pd.isna(val) else "N/A"
            elif arch == 'Kappa':
                val = subset[subset['MetricName'] == 'HistoryLoadToKafka']['ValueMs'].mean()
                ingest_val = f"{val / 1000.0:.2f} сек" if not pd.isna(val) else "N/A"
            elif arch == 'Lakehouse':
                val = subset[subset['MetricName'] == 'BatchLoadToDelta']['ValueMs'].mean()
                ingest_val = f"{val / 1000.0:.2f} сек" if not pd.isna(val) else "N/A"

            recalc_val = "N/A"
            if arch == 'Lambda':
                batch_runs = subset[subset['MetricName'] == 'BatchExecution']['ValueMs'].tolist()
                if len(batch_runs) > 1:
                    val = sum(batch_runs[1:]) / len(batch_runs[1:])
                else:
                    val = batch_runs[0] if batch_runs else None
                recalc_val = f"{val / 1000.0:.2f} сек" if not pd.isna(val) else "N/A"
            elif arch == 'Kappa':
                val = subset[subset['MetricName'] == 'HistoricalCatchUpStream']['ValueMs'].mean()
                recalc_val = f"{val / 1000.0:.2f} сек" if not pd.isna(val) else "N/A"

            lat_subset = subset[subset['MetricName'] == 'ServingQueryLatency']['ValueMs']
            if not lat_subset.empty:
                avg_lat = lat_subset.mean()
                latency_val = f"{avg_lat:.1f} мс"
            else:
                latency_val = "N/A"

            results.append({
                'Масштаб данных': size,
                'Архитектура': arch,
                'Загрузка истории': ingest_val,
                'Пересчет истории': recalc_val,
                'Задержка Serving-запроса': latency_val
            })

    report_df = pd.DataFrame(results)

    md_path = f"{output_dir}/benchmark_table.md"
    markdown_table = report_df.to_markdown(index=False)

    with open(md_path, 'w', encoding='utf-8') as f:
        f.write(markdown_table)
    print(f"[PYTHON] Таблица успешно сохранена в: {md_path}")

    print("\n" + "=" * 80)
    print("=" * 80)
    print(report_df.to_string(index=False))
    print("=" * 80)


if __name__ == '__main__':
    generate_tables()
