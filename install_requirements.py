import subprocess
import sys

# Lista de bibliotecas necessárias
required_packages = [
    'pika',
    'pandas',
    'mysql-connector-python'
]


for package in required_packages:
    try:
        __import__(package.split('-')[0])
    except ImportError:
        print(f"[INFO] Instalando: {package}")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
