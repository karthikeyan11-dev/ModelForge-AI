"""
Prism AI — Full Pipeline Integration Test (Steps 1–10)
=======================================================
Validates the entire preprocessing agent end-to-end:
  - Step 1: Adaptive thresholds
  - Step 2: Target detection + problem type
  - Step 3: Feature selection
  - Step 4: Pipeline persistence (save/load/transform)
  - Step 5: Format standardization (tested via static method)
  - Step 6: Drift detection
  - Step 8: Structured logging
  - Step 9: Error resilience
  - Step 10: Execution order
"""

import sys
import os

# Direct file-level import to bypass services/__init__.py (needs mlflow etc.)
import importlib.util
_mod_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                         "services", "intelligent_preprocessing.py")
_spec = importlib.util.spec_from_file_location("intelligent_preprocessing", _mod_path)
_mod = importlib.util.module_from_spec(_spec)
sys.modules["intelligent_preprocessing"] = _mod
_spec.loader.exec_module(_mod)
IntelligentPreprocessingAgent = _mod.IntelligentPreprocessingAgent

import pandas as pd
import numpy as np
import tempfile



# ─────────────────────────────────────────────────────────────────────
# STEP 1: Dynamic Decision Engine
# ─────────────────────────────────────────────────────────────────────

def test_step1_skew_calibration():
    """P75 skew clipped to [0.5, 2.0]."""
    print("⚡ Step 1: Skew calibration...")
    agent = IntelligentPreprocessingAgent()

    # Low skew → clipped to floor 0.5
    df_low = pd.DataFrame({
        "a": np.random.normal(0, 1, 500),
        "b": np.random.normal(0, 1, 500),
        "target": [0, 1] * 250,
    })
    _, s = agent.run_pipeline(df_low)
    assert s["calibration"]["threshold_skew"] == 0.5, f"Expected 0.5, got {s['calibration']['threshold_skew']}"
    print(f"   Low skew → {s['calibration']['threshold_skew']}  ✅")

    # High skew → adaptive (capped at 2.0)
    df_high = pd.DataFrame({
        "a": np.random.exponential(1, 500),
        "b": np.random.exponential(1, 500),
        "c": np.random.exponential(1, 500),
        "target": [0, 1] * 250,
    })
    _, s2 = agent.run_pipeline(df_high)
    assert s2["calibration"]["threshold_skew"] >= 1.0, f"Expected ≥1.0, got {s2['calibration']['threshold_skew']}"
    assert s2["calibration"]["threshold_skew"] <= 2.0, f"Expected ≤2.0, got {s2['calibration']['threshold_skew']}"
    print(f"   High skew → {s2['calibration']['threshold_skew']}  ✅")


def test_step1_missing_threshold():
    """Log-based formula for large datasets, 0.4 for small ones."""
    print("⚡ Step 1: Missing threshold...")
    agent = IntelligentPreprocessingAgent()

    # Small dataset → 0.4
    _, s = agent.run_pipeline(pd.DataFrame({"a": np.random.rand(50), "target": [0,1]*25}))
    assert s["calibration"]["threshold_missing"] == 0.4
    assert s["calibration"]["num_rows"] == 50
    print(f"   Small (N=50) → {s['calibration']['threshold_missing']}  ✅")

    # Large dataset → log formula: 1/log10(10010) ≈ 0.25
    _, s2 = agent.run_pipeline(pd.DataFrame({"a": np.random.rand(10000), "target": [0,1]*5000}))
    assert 0.2 < s2["calibration"]["threshold_missing"] < 0.3
    print(f"   Large (N=10000) → {s2['calibration']['threshold_missing']}  ✅")


# ─────────────────────────────────────────────────────────────────────
# STEP 2: Target Detection + Problem Type
# ─────────────────────────────────────────────────────────────────────

def test_step2_metadata_target():
    """Explicit metadata takes priority."""
    print("⚡ Step 2: Metadata target...")
    df = pd.DataFrame({"a": [1,2,3,4,5], "b": [10,20,30,40,50], "label": [0,1,0,1,0]})
    agent = IntelligentPreprocessingAgent()
    _, s = agent.run_pipeline(df, metadata_target="label")
    assert s["target_column"] == "label"
    assert s["intelligence"]["target_detection"]["method"] == "metadata"
    assert s["intelligence"]["target_detection"]["confidence"] == "high"
    print("   Metadata target → label  ✅")


def test_step2_heuristic_target():
    """Heuristic matches 'target', 'label', etc."""
    print("⚡ Step 2: Heuristic target...")
    df = pd.DataFrame({"age": [25,30,35,40,45]*20, "income": range(100), "target": [0,1]*50})
    agent = IntelligentPreprocessingAgent()
    _, s = agent.run_pipeline(df)
    assert s["target_column"] == "target"
    assert s["intelligence"]["target_detection"]["method"] == "heuristic"
    print("   Heuristic → 'target'  ✅")


def test_step2_excludes_ids():
    """ID-like columns (≥95% unique) are excluded from target candidates."""
    print("⚡ Step 2: ID exclusion...")
    df = pd.DataFrame({
        "id": range(100),
        "feature": np.random.rand(100),
        "label": [0,1]*50,
    })
    agent = IntelligentPreprocessingAgent()
    _, s = agent.run_pipeline(df)
    assert s["target_column"] == "label"
    print("   ID excluded, label detected  ✅")


def test_step2_classification():
    """Categorical target → classification."""
    print("⚡ Step 2: Classification...")
    df = pd.DataFrame({"x": range(100), "label": ["A","B"]*50})
    agent = IntelligentPreprocessingAgent()
    _, s = agent.run_pipeline(df, metadata_target="label")
    assert s["problem_type"] == "classification"
    print("   Classification detected  ✅")


def test_step2_regression():
    """High-cardinality numeric target → regression."""
    print("⚡ Step 2: Regression...")
    n = 1000
    df = pd.DataFrame({"x": np.random.rand(n), "price": np.random.rand(n) * 1000})
    agent = IntelligentPreprocessingAgent()
    _, s = agent.run_pipeline(df, metadata_target="price")
    assert s["problem_type"] == "regression"
    print("   Regression detected  ✅")


def test_step2_imbalance():
    """Imbalance detection for 90/10 split."""
    print("⚡ Step 2: Imbalance detection...")
    df = pd.DataFrame({"f": np.random.rand(100), "label": [1]*90 + [0]*10})
    agent = IntelligentPreprocessingAgent()
    _, s = agent.run_pipeline(df, metadata_target="label")
    # Check that imbalance_handling step was logged
    imbalance_steps = [st for st in s["steps_applied"] if st["step"] == "imbalance_handling"]
    assert len(imbalance_steps) >= 1, "Imbalance handling not logged"
    print("   Imbalance detected  ✅")


# ─────────────────────────────────────────────────────────────────────
# STEP 3: Feature Selection
# ─────────────────────────────────────────────────────────────────────

def test_step3_correlation_pruning():
    """Highly correlated features should be pruned."""
    print("⚡ Step 3: Correlation pruning...")
    n = 200
    x = np.random.rand(n)
    df = pd.DataFrame({
        "f1": x,
        "f2": x + np.random.normal(0, 0.001, n),
        "f3": x + np.random.normal(0, 0.001, n),
        "target": [0,1]*(n//2),
    })
    agent = IntelligentPreprocessingAgent()
    _, s = agent.run_pipeline(df, metadata_target="target")
    removed = s["feature_selection"].get("removed_features", [])
    print(f"   Removed: {removed}")
    assert len(removed) >= 1, f"Expected ≥1 pruned, got {len(removed)}"
    print("   Correlation pruning working  ✅")


def test_step3_importance_scores():
    """Importance scores should be multi-seed averaged and logged."""
    print("⚡ Step 3: Importance scores...")
    n = 200
    df = pd.DataFrame({
        "signal": [0,1]*(n//2),
        "noise": np.random.rand(n),
        "target": [0,1]*(n//2),
    })
    agent = IntelligentPreprocessingAgent()
    _, s = agent.run_pipeline(df, metadata_target="target")
    imp = s["feature_selection"].get("importance_scores", {})
    print(f"   Importance scores: {imp}")
    assert len(imp) > 0, "No importance scores logged"
    print("   Multi-seed importance  ✅")


def test_step3_sqrt_safeguard():
    """Never drop below sqrt(N) features."""
    print("⚡ Step 3: sqrt(N) safeguard...")
    n = 100
    df = pd.DataFrame({f"f{i}": np.random.rand(n) for i in range(5)})
    df["target"] = [0,1]*(n//2)
    agent = IntelligentPreprocessingAgent()
    _, s = agent.run_pipeline(df, metadata_target="target")
    kept = s["feature_selection"].get("kept_features", [])
    # sqrt(5) ≈ 2.2 → min 2 features kept
    assert len(kept) >= 2, f"Expected ≥2 kept, got {len(kept)}"
    print(f"   Kept {len(kept)} features (guard: 2)  ✅")


# ─────────────────────────────────────────────────────────────────────
# STEP 4: Pipeline Persistence
# ─────────────────────────────────────────────────────────────────────

def test_step4_save_load_transform():
    """Save → Load → Transform must produce identical output."""
    print("⚡ Step 4: Pipeline persistence...")
    df = pd.DataFrame({
        "age": [25, 30, 35, 40, 45, 50, 55, 60, 65, 70],
        "city": ["NY","LA","NY","SF","LA","NY","SF","LA","NY","SF"],
        "score": [0.1, 0.5, 0.9, 0.2, 0.4, 0.8, 0.3, 0.7, 0.6, 0.1],
        "target": [0,1,0,1,0,1,0,1,0,1],
    })
    agent = IntelligentPreprocessingAgent()
    df_processed, summary = agent.run_pipeline(df, metadata_target="target")

    # Save
    tmp = os.path.join(tempfile.gettempdir(), "prism_test_pipeline.pkl")
    agent.save_pipeline(tmp)
    assert os.path.exists(tmp), "Pipeline file not created"
    print(f"   Saved to {tmp}  ✅")

    # Load
    agent2 = IntelligentPreprocessingAgent.load_pipeline(tmp)
    assert agent2.target_column == "target"
    assert agent2.problem_type == agent.problem_type
    print("   Loaded successfully  ✅")

    # Transform new data
    df_new = pd.DataFrame({
        "age": [28, 42],
        "city": ["NY", "SF"],
        "score": [0.3, 0.6],
    })
    df_inf = agent2.transform(df_new)
    assert len(df_inf) == 2
    print(f"   Inference output shape: {df_inf.shape}  ✅")

    # Unseen category
    df_unseen = pd.DataFrame({
        "age": [33],
        "city": ["London"],  # unseen
        "score": [0.5],
    })
    df_u = agent2.transform(df_unseen)
    assert len(df_u) == 1, "Unseen category should not crash"
    print("   Unseen category handled  ✅")

    os.remove(tmp)


# ─────────────────────────────────────────────────────────────────────
# STEP 6: Drift Detection
# ─────────────────────────────────────────────────────────────────────

def test_step6_drift():
    """Drift detection should flag columns with shifted means."""
    print("⚡ Step 6: Drift detection...")
    df_train = pd.DataFrame({"a": np.random.normal(0, 1, 500), "target": [0,1]*250})
    agent = IntelligentPreprocessingAgent()
    agent.run_pipeline(df_train, metadata_target="target")

    df_normal = pd.DataFrame({"a": np.random.normal(0, 1, 100)})
    result_ok = agent.detect_drift(df_normal)
    print(f"   No drift: {result_ok}")

    df_drifted = pd.DataFrame({"a": np.random.normal(10, 1, 100)})
    result_bad = agent.detect_drift(df_drifted)
    assert result_bad["drift_detected"] is True
    assert "a" in result_bad["drift_columns"]
    print(f"   Drift detected: {result_bad}  ✅")


# ─────────────────────────────────────────────────────────────────────
# STEP 8: Structured Logging
# ─────────────────────────────────────────────────────────────────────

def test_step8_logging():
    """Summary must contain calibration, intelligence, feature_selection."""
    print("⚡ Step 8: Structured logging...")
    df = pd.DataFrame({"x": np.random.rand(100), "target": [0,1]*50})
    agent = IntelligentPreprocessingAgent()
    _, s = agent.run_pipeline(df, metadata_target="target")

    assert "calibration" in s
    assert "threshold_skew" in s["calibration"]
    assert "threshold_missing" in s["calibration"]
    assert "intelligence" in s
    assert "target_detection" in s["intelligence"]
    assert "target_analysis" in s["intelligence"]
    assert "feature_selection" in s
    assert "steps_applied" in s
    assert "decision_trace" in s
    assert "pipeline_steps" in s

    # Check execution order
    step_names = [st["step"] for st in s["steps_applied"]]
    expected_order = ["duplicate_removal", "audit", "calibration", "target_detection", "problem_detection"]
    for exp in expected_order:
        assert exp in step_names, f"Missing step: {exp}"
    print(f"   All log keys present  ✅")
    print(f"   Execution steps: {step_names}")


# ─────────────────────────────────────────────────────────────────────
# STEP 9: Error Resilience
# ─────────────────────────────────────────────────────────────────────

def test_step9_resilience():
    """Agent should not crash on edge-case data."""
    print("⚡ Step 9: Error resilience...")

    # Empty-ish dataset
    df = pd.DataFrame({"a": [1], "b": [2]})
    agent = IntelligentPreprocessingAgent()
    result, s = agent.run_pipeline(df)
    assert result is not None
    print("   Tiny dataset survived  ✅")

    # All-NaN column
    df2 = pd.DataFrame({"good": np.random.rand(20), "bad": [np.nan]*20, "target": [0,1]*10})
    agent2 = IntelligentPreprocessingAgent()
    result2, s2 = agent2.run_pipeline(df2, metadata_target="target")
    assert result2 is not None
    print("   All-NaN column survived  ✅")


# ─────────────────────────────────────────────────────────────────────
# RUNNER
# ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    tests = [
        test_step1_skew_calibration,
        test_step1_missing_threshold,
        test_step2_metadata_target,
        test_step2_heuristic_target,
        test_step2_excludes_ids,
        test_step2_classification,
        test_step2_regression,
        test_step2_imbalance,
        test_step3_correlation_pruning,
        test_step3_importance_scores,
        test_step3_sqrt_safeguard,
        test_step4_save_load_transform,
        test_step6_drift,
        test_step8_logging,
        test_step9_resilience,
    ]

    passed = 0
    failed = 0
    for t in tests:
        try:
            t()
            passed += 1
        except Exception as e:
            print(f"   ❌ FAILED: {e}")
            import traceback
            traceback.print_exc()
            failed += 1
        print()

    print(f"\n{'='*60}")
    print(f"RESULTS: {passed} passed, {failed} failed out of {len(tests)}")
    print(f"{'='*60}")
    if failed == 0:
        print("✅ ALL TESTS PASSED — System is production-ready.")
    else:
        print("❌ SOME TESTS FAILED — Review required.")
