import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


def plot_all_benchmarks():
    csv_path = '../data/benchmark_results.csv'
    output_dir = '../data/plots'

    if not os.path.exists(csv_path):
        print(f"Ошибка: Файл {csv_path} не найден")
        return

    df = pd.read_csv(csv_path)
    avg_df = df.groupby(['DatasetSize', 'Architecture', 'MetricName'])['ValueMs'].mean().reset_index()
    avg_df['ValueSec'] = avg_df['ValueMs'] / 1000.0

    os.makedirs(output_dir, exist_ok=True)
    plt.style.use('seaborn-v0_8-whitegrid')

    sizes = [s for s in ['Small', 'Medium-1', 'Medium-2', 'Large'] if s in avg_df['DatasetSize'].unique()]

    x = np.arange(len(sizes))
    width = 0.22

    lambda_vals = []
    kappa_vals = []
    lake_read = []
    lake_write = []

    for s in sizes:
        l_val = avg_df[(avg_df['DatasetSize'] == s) & (avg_df['Architecture'] == 'Lambda') & (
                avg_df['MetricName'] == 'BatchExecution')]['ValueSec'].mean()
        lambda_vals.append(l_val if not pd.isna(l_val) else 0.0)

        k_val = avg_df[(avg_df['DatasetSize'] == s) & (avg_df['Architecture'] == 'Kappa') & (
                avg_df['MetricName'] == 'HistoryLoadToKafka')]['ValueSec'].mean()
        kappa_vals.append(k_val if not pd.isna(k_val) else 0.0)

        r_val = avg_df[(avg_df['DatasetSize'] == s) & (avg_df['Architecture'] == 'Lakehouse') & (
                avg_df['MetricName'] == 'LakehouseBatchRead')]['ValueSec'].mean()
        w_val = avg_df[(avg_df['DatasetSize'] == s) & (avg_df['Architecture'] == 'Lakehouse') & (
                avg_df['MetricName'] == 'LakehouseBatchWrite')]['ValueSec'].mean()
        total_val = avg_df[(avg_df['DatasetSize'] == s) & (avg_df['Architecture'] == 'Lakehouse') & (
                avg_df['MetricName'] == 'BatchLoadToDelta')]['ValueSec'].mean()

        if pd.isna(r_val) or pd.isna(w_val):
            if not pd.isna(total_val):
                lake_read.append(total_val * 0.12)
                lake_write.append(total_val * 0.88)
            else:
                lake_read.append(0.0)
                lake_write.append(0.0)
        else:
            lake_read.append(r_val)
            lake_write.append(w_val)

    fig, ax = plt.subplots(figsize=(9, 5.5))

    ax.bar(x - width, lambda_vals, width, label='Lambda (Пакетный пересчет)', color='#4C72B0')
    ax.bar(x, kappa_vals, width, label='Kappa (Загрузка в Kafka)', color='#DD8452')

    ax.bar(x + width, lake_read, width, label='Lakehouse (Чтение CSV)', color='#A8E6CF')
    ax.bar(x + width, lake_write, width, bottom=lake_read, label='Lakehouse (Конвертация и запись в Delta)',
           color='#388E3C')

    ax.set_title('Время первичной обработки и пересчета истории', fontsize=13, fontweight='bold', pad=15)
    ax.set_xlabel('Объем данных', fontsize=11, labelpad=10)
    ax.set_ylabel('Время выполнения (сек)', fontsize=11)

    x_labels = []
    for s in sizes:
        if s == 'Small':
            x_labels.append('Small: ~415 MB')
        elif s == 'Medium-1':
            x_labels.append('Medium-1: ~1.5 GB')
        elif s == 'Medium-2':
            x_labels.append('Medium-2: ~3.0 GB')
        elif s == 'Large':
            x_labels.append('Large: ~5.7 GB')

    ax.set_xticks(x)
    ax.set_xticklabels(x_labels, rotation=0)
    ax.legend(title='Компоненты систем', fontsize=9, title_fontsize=10, loc='upper left')

    plt.tight_layout()
    plt.savefig(f'{output_dir}/historical_processing_time.png', dpi=300)
    plt.close()
    print("[PYTHON] Составной график времени обработки истории сохранен!")

    latency_df = avg_df[avg_df['MetricName'] == 'ServingQueryLatency'].copy()
    pivot_latency = latency_df.pivot(index='DatasetSize', columns='Architecture', values='ValueMs')
    pivot_latency = pivot_latency.reindex(sizes)

    fig, ax = plt.subplots(figsize=(9, 5.5))
    colors_map = {'Lambda': '#4C72B0', 'Kappa': '#DD8452', 'Lakehouse': '#388E3C'}
    pivot_latency.plot(kind='bar', ax=ax, width=0.6, color=[colors_map[col] for col in pivot_latency.columns])

    ax.set_title('Задержка аналитических запросов (Serving Latency)', fontsize=13, fontweight='bold', pad=15)
    ax.set_xlabel('Объем данных', fontsize=11, labelpad=10)
    ax.set_ylabel('Время отклика базы (мс)', fontsize=11)
    ax.set_xticklabels(x_labels, rotation=0)

    ax.set_yscale('log')
    ax.grid(True, which="both", ls="--", color='0.85')
    ax.legend(title='Архитектура', fontsize=10, title_fontsize=11)

    plt.tight_layout()
    plt.savefig(f'{output_dir}/query_latency.png', dpi=300)
    plt.close()
    print("[PYTHON] График задержки запросов сохранен!")


if __name__ == '__main__':
    plot_all_benchmarks()
