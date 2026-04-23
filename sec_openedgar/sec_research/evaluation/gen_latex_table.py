import pandas as pd

df = pd.read_csv('/home/ralbright/projects/openedgar/scratch/holdout_metrics_final.csv')
# Sort alphabetically or by F1? Alphabetically for an appendix is usually better.
df = df.sort_values(by='field')

latex_rows = []
for _, row in df.iterrows():
    # Escape underscores in field names
    field_esc = row['field'].replace('_', '\\_')
    line = f"{field_esc} & {row['precision']:.3f} & {row['recall']:.3f} & {row['f1']:.3f} \\\\"
    latex_rows.append(line)

print("\n".join(latex_rows))
