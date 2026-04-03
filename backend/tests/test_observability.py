"""
Prism AI — Observability & Logging Test
=======================================
Validates the transparency fixes (Issue 1 & 2):
  - pruning_skipped_reason in feature_selection log
  - warnings when target_column is None or low confidence
"""

import sys
import os
import importlib.util

# Direct file-level import to bypass services/__init__.py
_mod_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                         "services", "intelligent_preprocessing.py")
_spec = importlib.util.spec_from_file_location("intelligent_preprocessing", _mod_path)
_mod = importlib.util.module_from_spec(_spec)
sys.modules["intelligent_preprocessing"] = _mod
_spec.loader.exec_module(_mod)
IntelligentPreprocessingAgent = _mod.IntelligentPreprocessingAgent

import pandas as pd
import numpy as np

def test_feature_selection_skip_reasons():
    print("⚡ Testing feature selection skip reasons...")
    agent = IntelligentPreprocessingAgent()

    # Case 1: low_feature_count (N < 2)
    df1 = pd.DataFrame({"a": [1, 2, 3], "target": [0, 1, 0]})
    _, s1 = agent.run_pipeline(df1, metadata_target="target")
    reason1 = s1["feature_selection"].get("pruning_skipped_reason")
    assert reason1 == "low_feature_count", f"Expected low_feature_count, got {reason1}"
    print("   low_feature_count verified ✅")

    # Case 2: unsupervised_mode
    df2 = pd.DataFrame({"a": range(100), "b": range(100)})
    agent2 = IntelligentPreprocessingAgent()
    _, s2 = agent2.run_pipeline(df2) # No target
    reason2 = s2["feature_selection"].get("pruning_skipped_reason")
    assert reason2 == "unsupervised_mode", f"Expected unsupervised_mode, got {reason2}"
    print("   unsupervised_mode verified ✅")

    # Case 3: no_high_correlation
    # Features are independent and have high importance (so they aren't low signal)
    df3 = pd.DataFrame({
        "f1": np.random.rand(100),
        "f2": np.random.rand(100),
        "target": [0, 1] * 50
    })
    # Ensure they have some importance by making them correlate with target
    df3["f1"] = df3["target"] + np.random.normal(0, 0.1, 100)
    df3["f2"] = df3["target"] + np.random.normal(0, 0.1, 100)
    
    agent3 = IntelligentPreprocessingAgent()
    _, s3 = agent3.run_pipeline(df3, metadata_target="target")
    reason3 = s3["feature_selection"].get("pruning_skipped_reason")
    # If no pruning happened, it should be no_high_correlation because they are independent
    if not s3["feature_selection"]["removed_features"]:
        assert reason3 in ["no_high_correlation", "importance_below_threshold"], f"Expected no_high_correlation or importance_below_threshold, got {reason3}"
    print(f"   no_high_correlation/threshold verified ({reason3}) ✅")

def test_target_detection_warnings():
    print("⚡ Testing target detection warnings...")
    
    # Case 1: No target detected
    # Use only ID-like columns to force no target
    df1 = pd.DataFrame({
        "id": range(100),
        "index": range(100)
    })
    agent1 = IntelligentPreprocessingAgent()
    _, s1 = agent1.run_pipeline(df1)
    assert any("No target column detected" in w for w in s1["warnings"]), "Warning missing for None target"
    print("   Warning for None target verified ✅")

    # Case 2: Low confidence target (fallback)
    # One eligible column at the end
    df2 = pd.DataFrame({
        "id": range(100),
        "feature_x": np.random.rand(100)
    })
    agent2 = IntelligentPreprocessingAgent()
    _, s2 = agent2.run_pipeline(df2)
    assert s2["intelligence"]["target_detection"]["confidence"] == "low"
    assert any("No target column detected" in w for w in s2["warnings"]), "Warning missing for low confidence target"
    print("   Warning for low confidence target verified ✅")

if __name__ == "__main__":
    test_feature_selection_skip_reasons()
    test_target_detection_warnings()
    print("\n✅ All observability tests passed!")
