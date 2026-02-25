import pandas as pd
import matplotlib.pyplot as plt

bench = pd.read_csv("../results/benchmark.csv")
breakdown = pd.read_csv("../results/breakdown.csv")

# --- 1) Boxplot del recovery total (ms)
plt.figure()
plt.boxplot(bench["recovery_ms"].dropna())
plt.ylabel("Recovery time (ms)")
plt.title("Recovery time distribution (scripted crash/restart)")
plt.show()

# --- 2) Stacked bar de fases internas (processing->registered->started)
# Nos quedamos con filas que tienen las fases
b = breakdown.dropna(subset=["processing_to_registered_ms", "registered_to_started_ms"]).copy()

# Para que sea legible, mostramos solo primeras N iteraciones
N = min(10, len(b))
b = b.head(N)

x = range(len(b))

plt.figure()
plt.bar(x, b["processing_to_registered_ms"], label="Discover → Register")
plt.bar(x, b["registered_to_started_ms"], bottom=b["processing_to_registered_ms"], label="Register → Start")
plt.xticks(list(x), b["file"], rotation=45, ha="right")
plt.ylabel("Time (ms)")
plt.title("Recovery breakdown inside worker (first recovered query per run)")
plt.legend()
plt.tight_layout()
plt.show()

# --- 3) Comparación: recovery total vs interno (si quieres verlo)
plt.figure()
plt.scatter(b.index, b["processing_to_started_ms"])
plt.ylabel("Worker internal recovery (ms)")
plt.xlabel("Run index")
plt.title("Internal recovery time (processing → started)")
plt.show()

#4 RECOVERY BIEN
# Mostrar primeras 20 (o todas si <20)
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("../results/breakdown_full.csv").dropna(
    subset=["reconciler_to_processing_ms","processing_to_registered_ms","registered_to_started_ms"]
).copy()

# Mostrar primeras 20 (o todas si <20)
N = min(20, len(df))
df = df.head(N)

x = range(len(df))
a = df["reconciler_to_processing_ms"]
b = df["processing_to_registered_ms"]
c = df["registered_to_started_ms"]

plt.figure()
plt.bar(x, a, label="Reconciler → First discovery")
plt.bar(x, b, bottom=a, label="Discovery → Registered")
plt.bar(x, c, bottom=a+b, label="Registered → Started")

plt.xticks(list(x), df["file"], rotation=45, ha="right")
plt.ylabel("Time (ms)")
plt.title("Worker internal recovery breakdown (first recovered query)")
plt.legend()
plt.tight_layout()
plt.show()


#DEFINITIVO
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("../results/breakdown_rich.csv").copy()

# Solo filas con datos básicos
df = df.dropna(subset=[
    "boot_to_reconciler_ms",
    "reconciler_to_processing_ms",
    "processing_to_pipeline_ms",
    "pipeline_to_registered_ms",
    "registered_to_started_ms"
]).copy()

# Muestra máximo 20 para que sea legible
N = min(20, len(df))
df = df.head(N)

x = range(len(df))

a = df["boot_to_reconciler_ms"]
b = df["reconciler_to_processing_ms"]
c = df["processing_to_pipeline_ms"]
d = df["pipeline_to_registered_ms"]
e = df["registered_to_started_ms"]
# f es opcional (a veces no aparece)
f = df["started_to_filesink_ms"].fillna(0)

plt.figure()
plt.bar(x, a, label="Boot → Reconciler")
plt.bar(x, b, bottom=a, label="Reconciler → Discovery")
plt.bar(x, c, bottom=a+b, label="Discovery → Pipeline")
plt.bar(x, d, bottom=a+b+c, label="Pipeline → Registered")
plt.bar(x, e, bottom=a+b+c+d, label="Registered → Started")
plt.bar(x, f, bottom=a+b+c+d+e, label="Started → FileSink")

# Etiquetas más cortas
labels = [f"iter{i+1}" for i in range(len(df))]
plt.xticks(list(x), labels, rotation=0)

plt.ylabel("Time (ms)")
plt.title("Worker internal recovery breakdown (more phases)")
plt.legend()
plt.tight_layout()
plt.show()
