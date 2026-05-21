import pandas as pd
import numpy as np
import time
import tracemalloc
import os
from datetime import datetime, timedelta
import warnings

warnings.filterwarnings('ignore')

def create_test_dataframe(num_rows=500000):
    """Create a test DataFrame with 500,000 rows"""
    np.random.seed(42)
    
    print(f"Creating DataFrame with {num_rows:,} rows...")
    data = {
        'id': np.arange(1, num_rows + 1),
        'name': np.random.choice(['John', 'Jane', 'Bob', 'Alice', 'Charlie', 'Diana', 'Eve', 'Frank'], num_rows),
        'email': [f"user{i}@example.com" for i in range(num_rows)],
        'amount': np.random.uniform(10, 10000, num_rows),
        'date': [datetime.now() - timedelta(days=np.random.randint(0, 365)) for _ in range(num_rows)],
        'category': np.random.choice(['A', 'B', 'C', 'D', 'E'], num_rows)
    }
    
    df = pd.DataFrame(data)
    print(f"DataFrame created: {df.shape[0]:,} rows, {df.shape[1]} columns")
    print(f"DataFrame memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB\n")
    
    return df

def benchmark_format(df, format_name, filepath):
    """Benchmark saving and reading a specific format"""
    metrics = {
        'format': format_name,
        'file_size_mb': 0,
        'write_time_s': 0,
        'read_time_s': 0,
        'peak_memory_mb': 0,
        'cpu_time_s': 0,
        'energy_wh': 0
    }
    
    try:
        # Write benchmark
        tracemalloc.start()
        start_cpu = time.process_time()
        start_time = time.time()
        
        if format_name == 'CSV':
            df.to_csv(filepath, index=False)
        elif format_name == 'XLSX':
            df.to_excel(filepath, index=False, engine='openpyxl')
        elif format_name == 'Parquet':
            df.to_parquet(filepath, index=False, engine='pyarrow')
        elif format_name == 'ORC':
            df.to_orc(filepath, index=False)
        elif format_name == 'Feather':
            df.to_feather(filepath)
        
        write_time = time.time() - start_time
        write_cpu_time = time.process_time() - start_cpu
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        
        metrics['write_time_s'] = write_time
        metrics['cpu_time_s'] = write_cpu_time
        metrics['peak_memory_mb'] = peak / 1024**2
        
        # Get file size
        if os.path.exists(filepath):
            metrics['file_size_mb'] = os.path.getsize(filepath) / 1024**2
        
        # Read benchmark
        tracemalloc.start()
        start_cpu = time.process_time()
        start_time = time.time()
        
        if format_name == 'CSV':
            df_read = pd.read_csv(filepath)
        elif format_name == 'XLSX':
            df_read = pd.read_excel(filepath, engine='openpyxl')
        elif format_name == 'Parquet':
            df_read = pd.read_parquet(filepath, engine='pyarrow')
        elif format_name == 'ORC':
            df_read = pd.read_orc(filepath)
        elif format_name == 'Feather':
            df_read = pd.read_feather(filepath)
        
        read_time = time.time() - start_time
        read_cpu_time = time.process_time() - start_cpu
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        
        metrics['read_time_s'] = read_time
        metrics['cpu_time_s'] += read_cpu_time
        metrics['peak_memory_mb'] = max(metrics['peak_memory_mb'], peak / 1024**2)
        
        # Calculate energy consumption (CPU_time * 65W TDP / 3600 for Wh)
        metrics['energy_wh'] = metrics['cpu_time_s'] * 65 / 3600
        
        print(f"✓ {format_name:10} - File: {metrics['file_size_mb']:8.2f} MB | "
              f"Write: {metrics['write_time_s']:6.2f}s | "
              f"Read: {metrics['read_time_s']:6.2f}s")
        
        return metrics
        
    except Exception as e:
        print(f"✗ {format_name:10} - Error: {str(e)}")
        return None

def main():
    # Create test data
    df = create_test_dataframe(500000)
    
    # Create output directory if needed
    output_dir = 'benchmark_output'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Benchmark each format
    print("=" * 80)
    print("BENCHMARKING FILE FORMATS")
    print("=" * 80 + "\n")
    
    formats = ['CSV', 'Parquet', 'ORC', 'Feather']
    results = []
    
    for fmt in formats:
        filepath = os.path.join(output_dir, f'benchmark.{fmt.lower()}')
        print(f"Benchmarking {fmt}...")
        
        metrics = benchmark_format(df, fmt, filepath)
        if metrics:
            results.append(metrics)
        
        # Clean up
        if os.path.exists(filepath):
            try:
                os.remove(filepath)
            except:
                pass
        print()
    
    # Create results DataFrame
    results_df = pd.DataFrame(results)
    
    # Calculate CSV baseline for comparison
    csv_metrics = results_df[results_df['format'] == 'CSV'].iloc[0]
    csv_size = csv_metrics['file_size_mb']
    csv_write_time = csv_metrics['write_time_s']
    csv_read_time = csv_metrics['read_time_s']
    csv_energy = csv_metrics['energy_wh']
    
    # Add percentage savings columns
    results_df['size_savings_%'] = ((csv_size - results_df['file_size_mb']) / csv_size * 100).round(2)
    results_df['write_time_savings_%'] = ((csv_write_time - results_df['write_time_s']) / csv_write_time * 100).round(2)
    results_df['read_time_savings_%'] = ((csv_read_time - results_df['read_time_s']) / csv_read_time * 100).round(2)
    results_df['energy_savings_%'] = ((csv_energy - results_df['energy_wh']) / csv_energy * 100).round(2)
    
    # Print detailed comparison table
    print("=" * 120)
    print("FILE FORMAT BENCHMARKING RESULTS (500,000 rows)")
    print("=" * 120)
    print("\n📊 CORE METRICS:")
    print("-" * 120)
    
    core_cols = ['format', 'file_size_mb', 'write_time_s', 'read_time_s', 'peak_memory_mb', 'cpu_time_s', 'energy_wh']
    for idx, row in results_df.iterrows():
        print(f"{row['format']:10} | "
              f"Size: {row['file_size_mb']:8.2f} MB | "
              f"Write: {row['write_time_s']:7.3f}s | "
              f"Read: {row['read_time_s']:7.3f}s | "
              f"Peak Mem: {row['peak_memory_mb']:8.2f} MB | "
              f"CPU: {row['cpu_time_s']:7.3f}s | "
              f"Energy: {row['energy_wh']:7.4f} Wh")
    
    print("\n💾 PERCENTAGE SAVINGS VS CSV BASELINE:")
    print("-" * 120)
    for idx, row in results_df.iterrows():
        if row['format'] != 'CSV':
            print(f"{row['format']:10} | "
                  f"Size: {row['size_savings_%']:+7.2f}% | "
                  f"Write: {row['write_time_savings_%']:+7.2f}% | "
                  f"Read: {row['read_time_savings_%']:+7.2f}% | "
                  f"Energy: {row['energy_savings_%']:+7.2f}%")
    
    print("\n" + "=" * 120)
    
    # Summary statistics
    print("\n📈 SUMMARY STATISTICS:")
    print("-" * 120)
    
    fastest_write = results_df.loc[results_df['write_time_s'].idxmin()]
    fastest_read = results_df.loc[results_df['read_time_s'].idxmin()]
    smallest_file = results_df.loc[results_df['file_size_mb'].idxmin()]
    lowest_energy = results_df.loc[results_df['energy_wh'].idxmin()]
    
    print(f"Fastest Write:     {fastest_write['format']:10} ({fastest_write['write_time_s']:.3f}s)")
    print(f"Fastest Read:      {fastest_read['format']:10} ({fastest_read['read_time_s']:.3f}s)")
    print(f"Smallest File:     {smallest_file['format']:10} ({smallest_file['file_size_mb']:.2f} MB)")
    print(f"Lowest Energy:     {lowest_energy['format']:10} ({lowest_energy['energy_wh']:.4f} Wh)")
    
    print("\n" + "=" * 120)
    
    # Detailed table for export
    print("\n📋 DETAILED RESULTS TABLE:")
    print("-" * 120)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)
    
    display_df = results_df[[
        'format', 'file_size_mb', 'write_time_s', 'read_time_s', 
        'peak_memory_mb', 'cpu_time_s', 'energy_wh', 'size_savings_%'
    ]].copy()
    
    print(display_df.to_string(index=False))
    
    print("\n" + "=" * 120)
    print("\n✅ Benchmark completed successfully!")
    print("\nMetrics Explanation:")
    print("  • File Size (MB):        Compressed file size on disk")
    print("  • Write Time (s):        Time to save DataFrame to file")
    print("  • Read Time (s):         Time to load file back to DataFrame")
    print("  • Peak Memory (MB):      Maximum memory used during operation")
    print("  • CPU Time (s):          CPU processing time for write + read")
    print("  • Energy (Wh):           Estimated energy consumption (CPU_time × 65W ÷ 3600)")
    print("  • Size Savings (%):      Compression ratio vs CSV format")

if __name__ == "__main__":
    main()
