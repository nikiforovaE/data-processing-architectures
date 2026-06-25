import os
import re
import pandas as pd
import matplotlib.pyplot as plt


def parse_cpu(cpu_str):
    try:
        return float(cpu_str.replace('%', '').strip())
    except:
        return 0.0


def parse_mem_to_mb(mem_str):
    try:
        val_part = mem_str.split('/')[0].strip()
        match = re.search(r"([0-9.]+)\s*([a-zA-Z]+)", val_part)
        if not match:
            return 0.0
        val = float(match.group(1))
        unit = match.group(2).lower()
        if 'g' in unit:
            return val * 1024.0
        elif 'k' in unit:
            return val / 1024.0
        elif 'm' in unit:
            return val
        return val / (1024.0 * 1024.0)
    except:
        return 0.0


def parse_io_to_mb(io_str):
    try:
        parts = io_str.split('/')
        if len(parts) < 2:
            return 0.0, 0.0

        def to_mb(s):
            match = re.search(r"([0-9.]+)\s*([a-zA-Z]+)", s.strip())
            if not match:
                return 0.0
            val = float(match.group(1))
            unit = match.group(2).lower()
            if 'g' in unit:
                return val * 1024.0
            elif 'k' in unit:
                return val / 1024.0
            elif 'm' in unit:
                return val
            return val / (1024.0 * 1024.0)

        return to_mb(parts[0]), to_mb(parts[1])
    except:
        return 0.0, 0.0


def process_and_plot():
    csv_path = '../data/plots/resource_stats.csv'
    output_dir = '../data/plots'

    if not os.path.exists(csv_path):
        csv_path = 'data/plots/resource_stats.csv'
        output_dir = 'data/plots'

    if not os.path.exists(csv_path):
        print(f"Ошибка: Файл {csv_path} не найден.")
        return

    df = pd.read_csv(csv_path)

    print("[PYTHON] Парсинг сырых логов ресурсов...")
    df['CPU_Clean'] = df['CPU'].apply(parse_cpu)
    df['MEM_MB'] = df['MEM'].apply(parse_mem_to_mb)

    parsed_io = df['BlockIO'].apply(parse_io_to_mb)
    df['IO_Read_MB'] = [x[0] for x in parsed_io]
    df['IO_Write_MB'] = [x[1] for x in parsed_io]

    df['TimeSec'] = df.groupby(['Architecture', 'DatasetSize', 'Container']).cumcount() * 2

    os.makedirs(output_dir, exist_ok=True)
    plt.style.use('seaborn-v0_8-whitegrid')

    for size in df['DatasetSize'].unique():
        size_df = df[df['DatasetSize'] == size].copy()
        if size_df.empty:
            continue

        fig, ax = plt.subplots(figsize=(10, 5))
        has_data = False
        for arch in ['lambda', 'kappa', 'lakehouse']:
            arch_df = size_df[size_df['Architecture'] == arch].groupby('TimeSec')['CPU_Clean'].sum().reset_index()
            if not arch_df.empty:
                ax.plot(arch_df['TimeSec'], arch_df['CPU_Clean'], label=arch.upper(), linewidth=2)
                has_data = True

        if has_data:
            ax.set_title(f'Общая загрузка CPU во времени (масштаб: {size})', fontsize=12, fontweight='bold', pad=15)
            ax.set_xlabel('Время эксперимента (сек)', fontsize=10)
            ax.set_ylabel('Загрузка процессора (%)', fontsize=10)
            ax.legend(title='Архитектура', fontsize=9)
            plt.tight_layout()
            plt.savefig(f'{output_dir}/cpu_over_time_{size}.png', dpi=300)
        plt.close()

        fig, ax = plt.subplots(figsize=(10, 5))
        has_data = False
        for arch in ['lambda', 'kappa', 'lakehouse']:
            arch_df = size_df[size_df['Architecture'] == arch].groupby('TimeSec')['MEM_MB'].sum().reset_index()
            if not arch_df.empty:
                ax.plot(arch_df['TimeSec'], arch_df['MEM_MB'], label=arch.upper(), linewidth=2)
                has_data = True

        if has_data:
            ax.set_title(f'Общее потребление оперативной памяти (масштаб: {size})', fontsize=12, fontweight='bold',
                         pad=15)
            ax.set_xlabel('Время эксперимента (сек)', fontsize=10)
            ax.set_ylabel('Потребление ОЗУ (МБ)', fontsize=10)
            ax.legend(title='Архитектура', fontsize=9)
            plt.tight_layout()
            plt.savefig(f'{output_dir}/memory_over_time_{size}.png', dpi=300)
        plt.close()

    summary_data = []
    for size in df['DatasetSize'].unique():
        for arch in ['lambda', 'kappa', 'lakehouse']:
            sub = df[(df['DatasetSize'] == size) & (df['Architecture'] == arch)]
            if sub.empty:
                continue

            time_grouped = sub.groupby('TimeSec').agg({
                'CPU_Clean': 'sum',
                'MEM_MB': 'sum',
                'IO_Read_MB': 'sum',
                'IO_Write_MB': 'sum'
            }).reset_index()

            avg_cpu = time_grouped['CPU_Clean'].mean()
            max_cpu = time_grouped['CPU_Clean'].max()
            max_mem = time_grouped['MEM_MB'].max()

            total_read = sub.groupby('Container')['IO_Read_MB'].max().sum()
            total_write = sub.groupby('Container')['IO_Write_MB'].max().sum()

            summary_data.append({
                'Масштаб': size,
                'Архитектура': arch.upper(),
                'Среднее CPU (%)': f"{avg_cpu:.1f}%",
                'Пиковое CPU (%)': f"{max_cpu:.1f}%",
                'Пиковое ОЗУ (МБ)': f"{max_mem:.1f} МБ",
                'Суммарный ввод (Read)': f"{total_read:.1f} МБ",
                'Суммарный вывод (Write)': f"{total_write:.1f} МБ"
            })

    summary_df = pd.DataFrame(summary_data)
    md_path = f"{output_dir}/resources_summary_table.md"
    summary_df.to_markdown(md_path, index=False)

    print("\n" + "=" * 80)
    print("СВОДНЫЙ АНАЛИЗ ИСПОЛЬЗОВАНИЯ АППАРАТНЫХ РЕСУРСОВ")
    print("=" * 80)
    print(summary_df.to_string(index=False))
    print("=" * 80)
    print(f"[PYTHON] Итоговая аналитическая таблица сохранена в: {md_path}")
    print("[PYTHON] Графики динамики успешно сохранены!")


if __name__ == '__main__':
    process_and_plot()
