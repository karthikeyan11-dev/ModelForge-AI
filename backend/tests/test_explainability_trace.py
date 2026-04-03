import pandas as pd
import numpy as np
import json
from services.intelligent_preprocessing import IntelligentPreprocessingAgent

def test_explainability_and_trace():
    print("🚀 Starting Explainability & Trace Verification...")
    
    # Dataset 1: Simple Balanced Classification (Should skip Scaling & Imbalance)
    data = {
        'age': [20, 25, 30, 35, 40] * 10,
        'target': [0, 1, 0, 1, 0] * 10
    }
    df = pd.DataFrame(data)
    agent = IntelligentPreprocessingAgent()
    _, summary = agent.run_pipeline(df)
    
    # 1. Check Mode & Confidence
    assert summary["mode"] == "supervised"
    assert summary["target_detection"]["confidence"] == "medium" # Heuristic 'target'
    
    # 2. Check Decision Trace (Applied vs Skipped)
    trace = summary["decision_trace"]
    # Scaling should be skipped since std is low (age std is ~7)
    scaling_step = next(t for t in trace if t["step"] == "feature_scaling")
    assert scaling_step["applied"] == False
    assert "within range" in scaling_step["reason"]
    
    # Imbalance should be skipped (50/50 split)
    imbal_step = next(t for t in trace if t["step"] == "imbalance_handling")
    assert imbal_step["applied"] == False
    
    # 3. Check Target Analysis
    assert "0" in summary["target_analysis"]["distribution"]
    assert summary["target_analysis"]["distribution"]["0"] == 0.6 # [0,1,0,1,0] has 3 zeros out of 5
    
    # Dataset 2: Unsupervised Mode (ID column last)
    data_unsup = {
        'feat': np.random.rand(100),
        'id': range(100)
    }
    _, summary_unsup = agent.run_pipeline(pd.DataFrame(data_unsup))
    assert summary_unsup["mode"] == "unsupervised"
    assert summary_unsup["target_column"] is None
    
    # Dataset 3: Metadata Explicit Target (High Confidence)
    _, summary_meta = agent.run_pipeline(pd.DataFrame(data), metadata_target="age")
    assert summary_meta["target_column"] == "age"
    assert summary_meta["target_detection"]["confidence"] == "high"

    print("✅ Explainability & Trace Tests Passed!")
    print(f"Sample Trace Node: {json.dumps(trace[0], indent=2)}")

if __name__ == "__main__":
    test_explainability_and_trace()
