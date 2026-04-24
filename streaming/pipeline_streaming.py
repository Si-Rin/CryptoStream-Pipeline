"""
Pipeline Streaming avec Kafka
Lance tous les scripts streaming en parallèle
"""
import subprocess
import sys
import threading
import time

def run_script(script):
    print(f"🚀 Lancement de {script}...")
    subprocess.run([sys.executable, script])

if __name__ == "__main__":
    print("\n" + "="*50)
    print("🚀 PIPELINE STREAMING DÉMARRÉ")
    print("="*50)

    # Lancer Bronze, Clean, Silver en parallèle
    # car ils tournent en continu
    t1 = threading.Thread(
        target=run_script,
        args=("streaming/spark/bronze_streaming.py",)
    )
    t2 = threading.Thread(
        target=run_script,
        args=("streaming/spark/clean_streaming.py",)
    )
    t3 = threading.Thread(
        target=run_script,
        args=("streaming/spark/silver_streaming.py",)
    )

    t1.start()
    t2.start()
    t3.start()

    print("✅ Bronze + Clean + Silver streaming démarrés !")
    print("⏳ Gold se met à jour toutes les 60 secondes...")

    # Gold tourne en batch toutes les 60 secondes
    while True:
        print("\n⚙️ Mise à jour Gold Layer...")
        subprocess.run([
            sys.executable,
            "streaming/spark/gold_streaming.py"
        ])
        print("✅ Gold mis à jour !")
        print("⏳ Prochain update dans 60 secondes...")
        time.sleep(60)