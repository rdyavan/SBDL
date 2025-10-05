"""
COMPLETE FIX FOR PYSPARK ON WINDOWS
This script addresses all the issues we've encountered:
1. Python version mismatch (3.11 vs 3.12)
2. Spark 4.0 compatibility issues
3. Log4j configuration problems
"""

import os
import sys
import subprocess

print("=" * 70)
print("PYSPARK WINDOWS SETUP - COMPLETE FIX")
print("=" * 70)

# Step 1: Check current Python version
print(f"\n1. Current Python: {sys.executable}")
print(f"   Version: {sys.version}")

# Step 2: Fix environment variables BEFORE any Spark imports
print("\n2. Fixing environment variables...")

# Remove Python 3.11 from PATH
path_dirs = os.environ.get('PATH', '').split(os.pathsep)
filtered_dirs = []
removed_paths = []

for path_dir in path_dirs:
    if any(pattern in path_dir.lower() for pattern in ['3.11', 'python311', 'py311']):
        removed_paths.append(path_dir)
    else:
        filtered_dirs.append(path_dir)

if removed_paths:
    print(f"   Removed {len(removed_paths)} Python 3.11 paths from PATH")

# Add current Python to front of PATH
current_python_dir = os.path.dirname(sys.executable)
os.environ['PATH'] = current_python_dir + os.pathsep + os.pathsep.join(filtered_dirs)

# Set PySpark Python variables
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Fix Log4j configuration issues
os.environ['SPARK_LOG_DIR'] = 'C:\\tmp\\spark-logs'
os.environ['SPARK_LOCAL_DIRS'] = 'C:\\tmp\\spark-local'

# Create directories if they don't exist
for dir_path in ['C:\\tmp\\spark-logs', 'C:\\tmp\\spark-local', 'C:\\tmp\\spark-warehouse']:
    os.makedirs(dir_path, exist_ok=True)
    print(f"   Created/verified: {dir_path}")

print("   ✓ Environment configured")

# Step 3: Check PySpark version
print("\n3. Checking PySpark installation...")
try:
    result = subprocess.run(
        [sys.executable, '-m', 'pip', 'show', 'pyspark'],
        capture_output=True,
        text=True
    )
    if 'Version:' in result.stdout:
        version_line = [line for line in result.stdout.split('\n') if 'Version:' in line][0]
        version = version_line.split(':')[1].strip()
        print(f"   Current PySpark version: {version}")

        if version.startswith('4.'):
            print("   ⚠ WARNING: Spark 4.x has known issues on Windows")
            print("   Recommendation: Downgrade to 3.5.1")
            print("\n   Run this command:")
            print("   pip uninstall pyspark && pip install pyspark==3.5.1")

            response = input("\n   Would you like to continue anyway? (yes/no): ")
            if response.lower() not in ['yes', 'y']:
                sys.exit(0)
    else:
        print("   PySpark not found!")
        print("   Install with: pip install pyspark==3.5.1")
        sys.exit(1)

except Exception as e:
    print(f"   Error checking PySpark: {e}")

# Step 4: Create Spark session
print("\n4. Creating Spark session...")

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark import SparkConf

try:
    # Comprehensive configuration
    conf = SparkConf()
    conf.set("spark.pyspark.python", sys.executable)
    conf.set("spark.pyspark.driver.python", sys.executable)
    conf.set("spark.sql.warehouse.dir", "file:///C:/tmp/spark-warehouse")
    conf.set("spark.local.dir", "C:/tmp/spark-local")

    # Disable problematic features
    conf.set("spark.sql.adaptive.enabled", "false")
    conf.set("spark.hadoop.io.native.lib.available", "false")
    conf.set("spark.ui.enabled", "false")  # Disable UI to avoid port conflicts

    # Logging configuration
    conf.set("spark.log.level", "ERROR")

    spark = SparkSession.builder \
        .appName("SparkWindowsFix") \
        .config(conf=conf) \
        .master("local[1]") \
        .getOrCreate()

    # Set log level
    spark.sparkContext.setLogLevel("ERROR")

    print("   ✓ Spark session created successfully!")

    # Step 5: Test DataFrame operations
    print("\n5. Testing DataFrame operations...")

    # Create schema
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])

    # Create test data
    data = [
        (1, "Alice", 25),
        (2, "Bob", 30),
        (3, "Charlie", 35)
    ]

    # Create DataFrame
    df = spark.createDataFrame(data, schema=schema)

    print("   ✓ DataFrame created")
    print("\n   Schema:")
    df.printSchema()

    print("\n   Data:")
    df.show()

    # Test operations
    print("\n6. Testing operations...")
    print(f"   Row count: {df.count()}")

    print("\n   Filtered (age > 27):")
    df.filter(df.age > 27).show()

    print("\n   Selected columns:")
    df.select("name", "age").show()

    print("\n" + "=" * 70)
    print("✓ ALL TESTS PASSED SUCCESSFULLY!")
    print("=" * 70)

    print("\n📋 SUMMARY:")
    print(f"   Python: {sys.version.split()[0]}")
    print(f"   PySpark: {version}")
    print(f"   Everything is working correctly!")

except Exception as e:
    print(f"\n❌ ERROR: {e}")
    print("\n🔧 TROUBLESHOOTING:")
    print("1. Uninstall PySpark: pip uninstall pyspark")
    print("2. Install stable version: pip install pyspark==3.5.1")
    print("3. Remove Spark 4.0 folder: rename D:\\spark-4.0.0-bin-hadoop3")
    print("4. Run this script again")

    import traceback

    print("\nFull error traceback:")
    traceback.print_exc()

finally:
    # Cleanup
    try:
        spark.stop()
        print("\n✓ Spark session stopped")
    except:
        pass

print("\n📝 TO USE IN YOUR PROJECTS:")
print("""
# Add this at the start of your PySpark scripts:
import os, sys

# Fix Python PATH
path_dirs = os.environ['PATH'].split(os.pathsep)
filtered_dirs = [d for d in path_dirs if '3.11' not in d.lower()]
os.environ['PATH'] = os.path.dirname(sys.executable) + os.pathsep + os.pathsep.join(filtered_dirs)
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Create necessary directories
os.makedirs('C:\\\\tmp\\\\spark-warehouse', exist_ok=True)

# Your Spark code here...
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MyApp").master("local[*]").getOrCreate()
""")