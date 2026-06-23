import os
import pandas as pd
import matplotlib.pyplot as plt


def plot_all_benchmarks():
    csv_path = '../data/benchmark_results.csv'
    output_dir = '../data/plots'

    if not os.path.exists(csv_path):
        print(f"Ошибка: Файл {csv_path} не найден")
        return

    df = pd.read_csv(csv_path)

    avg_df = df.groupby(['DatasetSize', 'Architecture', 'MetricName'])['ValueMs'].mean().reset_index()

    os.makedirs(output_dir, exist_ok=True)

    plt.style.use('seaborn-v0_8-whitegrid')

    colors = {'Lambda': '#4C72B0', 'Kappa': '#DD8452', 'Lakehouse': '#55A868'}

    recomp_metrics = ['BatchExecution', 'HistoricalCatchUpStream', 'BatchLoadToDelta']
    recomp_df = avg_df[avg_df['MetricName'].isin(recomp_metrics)].copy()

    recomp_df['ValueSec'] = recomp_df['ValueMs'] / 1000.0

    pivot_recomp = recomp_df.pivot(index='DatasetSize', columns='Architecture', values='ValueSec')
    pivot_recomp = pivot_recomp.reindex(['Small', 'Large'])

    fig, ax = plt.subplots(figsize=(8, 5))
    pivot_recomp.plot(kind='bar', ax=ax, width=0.6, color=[colors[col] for col in pivot_recomp.columns])

    ax.set_title('Время пересчета / обработки истории', fontsize=13, fontweight='bold', pad=15)
    ax.set_xlabel('Объем данных', fontsize=11, labelpad=10)
    ax.set_ylabel('Время выполнения (сек)', fontsize=11)
    ax.set_xticklabels(['Small: ~415 MB', 'Large: ~5.7 GB'], rotation=0)
    ax.legend(title='Архитектура', fontsize=10, title_fontsize=11)

    plt.tight_layout()
    plt.savefig(f'{output_dir}/historical_processing_time.png', dpi=300)
    plt.close()
    print("[PYTHON] График времени обработки истории сохранен в: " + f"{output_dir}/historical_processing_time.png")

    latency_df = avg_df[avg_df['MetricName'] == 'ServingQueryLatency'].copy()

    pivot_latency = latency_df.pivot(index='DatasetSize', columns='Architecture', values='ValueMs')
    pivot_latency = pivot_latency.reindex(['Small', 'Large'])

    fig, ax = plt.subplots(figsize=(8, 5))
    pivot_latency.plot(kind='bar', ax=ax, width=0.6, color=[colors[col] for col in pivot_latency.columns])

    ax.set_title('Задержка аналитических запросов', fontsize=13, fontweight='bold', pad=15)
    ax.set_xlabel('Объем данных', fontsize=11, labelpad=10)
    ax.set_ylabel('Время отклика базы (мс)', fontsize=11)
    ax.set_xticklabels(['Small: ~415 MB', 'Large: ~5.7 GB'], rotation=0)

    ax.set_yscale('log')
    ax.grid(True, which="both", ls="--", color='0.85')
    ax.legend(title='Архитектура', fontsize=10, title_fontsize=11)

    plt.tight_layout()
    plt.savefig(f'{output_dir}/query_latency.png', dpi=300)
    plt.close()
    print("[PYTHON] График задержки запросов сохранен в: " + f"{output_dir}/query_latency.png")


if __name__ == '__main__':
    plot_all_benchmarks()
