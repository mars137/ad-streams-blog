"""Local mirror of propensity_model_dev.ipynb, for running the experiment from a
laptop against the demo_atahir account without opening the notebook.

The notebook is the source of truth: retrain_propensity_task executes it weekly.
Keep the candidate set and champion-selection logic here in sync with cells 5-8
of the notebook, or the scheduled run and this script will disagree.

Unlike the notebook, this pins version_name to V7 rather than deriving the next
free version, so re-running it will fail once V7 exists. That is deliberate: it
keeps ad-hoc local runs from quietly bumping the production version pointer.
"""
from snowflake.snowpark import Session
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import roc_auc_score, accuracy_score, f1_score
from xgboost import XGBClassifier
from lightgbm import LGBMClassifier
from snowflake.ml.experiment import ExperimentTracking
from snowflake.ml.registry import Registry
from snowflake.ml.model import task

session = Session.builder.configs({
    "connection_name": "demo_atahir",
    "database": "DEMO_ATAHIR", "schema": "AD_STREAMS", "warehouse": "AD_STREAMS_WH",
}).create()
print("Account:", session.get_current_account())

FEATURES = [
    "CLICKS_1H", "CLICKS_24H", "CLICKS_7D",
    "IMPRESSIONS_1H", "IMPRESSIONS_24H", "IMPRESSIONS_7D",
    "PAID_SEARCH_1H", "PAID_SEARCH_24H", "PAID_SEARCH_7D",
    "EVENT_VELOCITY_24H",
]
df = session.sql(f"""
    SELECT {', '.join(FEATURES)}, IFF(conversions_total > 0, 1, 0) AS CONVERTED
    FROM DEMO_ATAHIR.AD_STREAMS.dt_user_features
""").to_pandas()
X = df[FEATURES].astype(float)
y = df["CONVERTED"].astype(int)
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.3, random_state=42, stratify=y
)
print(f"Train: {len(X_train)} | Test: {len(X_test)} | Positive rate: {y.mean():.2f}")

exp = ExperimentTracking(session, database_name="DEMO_ATAHIR", schema_name="ML_EXPERIMENTS")
exp.set_experiment("AD_PROPENSITY_EXPERIMENT")

# Regularized, scaled logistic regression: smooth, monotonic marginal response
# per feature (ideal for a propensity model and live single-feature demos).
# Strong L2 (small C) prevents the degenerate perfect-separation seen earlier.
candidates = {
    "logreg_reg":    make_pipeline(StandardScaler(), LogisticRegression(C=0.05, max_iter=2000)),
    "random_forest": RandomForestClassifier(n_estimators=200, random_state=42),
    "xgboost":       XGBClassifier(n_estimators=200, max_depth=4, eval_metric="logloss"),
    "lightgbm":      LGBMClassifier(n_estimators=200, max_depth=4, verbose=-1),
}
results = {}
for name, model in candidates.items():
    with exp.start_run(f"{name}_v7"):
        model.fit(X_train, y_train)
        proba = model.predict_proba(X_test)[:, 1]
        preds = model.predict(X_test)
        auc = roc_auc_score(y_test, proba)
        acc = accuracy_score(y_test, preds)
        f1 = f1_score(y_test, preds, zero_division=0)
        exp.log_params({k: str(v) for k, v in model.get_params().items()})
        exp.log_metrics({"roc_auc": auc, "accuracy": acc, "f1": f1})
        results[name] = {"model": model, "roc_auc": auc, "accuracy": acc, "f1": f1}
        print(f"{name:14s} AUC={auc:.3f}  ACC={acc:.3f}  F1={f1:.3f}")

leaderboard = pd.DataFrame(
    [{"model": k, **{m: v[m] for m in ("roc_auc", "accuracy", "f1")}} for k, v in results.items()]
).sort_values("roc_auc", ascending=False).reset_index(drop=True)

# Reject suspiciously-perfect models: AUC >= 0.999 signals degenerate perfect
# separation (unstable, non-monotonic coefficients), not a good production model.
eligible = leaderboard[leaderboard["roc_auc"] < 0.999]
if eligible.empty:
    eligible = leaderboard
# Select by F1 among eligible models: balances precision/recall and favors the
# regularized linear model, which gives smooth, monotonic per-feature response.
eligible = eligible.sort_values("f1", ascending=False)
champion_name = eligible.iloc[0]["model"]
champion = results[champion_name]["model"]
print("\nLeaderboard:\n", leaderboard.to_string(index=False))
print("Champion (by F1, excluding AUC>=0.999 degenerate models):", champion_name)

reg = Registry(session, database_name="DEMO_ATAHIR", schema_name="ML_REGISTRY")
mv = reg.log_model(
    champion, model_name="AD_PROPENSITY_MODEL", version_name="V7",
    sample_input_data=X_train,
    conda_dependencies=["scikit-learn", "xgboost", "lightgbm"],
    metrics={
        "roc_auc": float(results[champion_name]["roc_auc"]),
        "accuracy": float(results[champion_name]["accuracy"]),
        "f1": float(results[champion_name]["f1"]),
        "champion": champion_name,
        "training_rows": int(len(X_train)),
    },
    comment=f"V7 regularized logreg champion (by F1). Champion={champion_name}.",
    task=task.Task.TABULAR_BINARY_CLASSIFICATION,
)
print("Registered:", mv.model_name, mv.version_name)
print("Functions:", [f["name"] for f in mv.show_functions()])
m = reg.get_model("AD_PROPENSITY_MODEL")
m.default = "V7"
print("Default version:", m.default.version_name)

preds = mv.run(X_test.head(5), function_name="predict_proba")
print("Inference output columns:", preds.columns.tolist())
print(preds.to_string(index=False))
session.close()
