"""
scripts/gen_clean_baseline.py

Generates a purely clean baseline dataset with zero injected incidents.
This is used to perform a cold start on the Isolation Forest model so it can
learn the mathematical boundaries of a "healthy" network.
"""

import os
import sys
import random
from datetime import datetime, timedelta

# Import from the existing generator
from gen_vendor_neutral_logs import RECIPES, _gen_scenario

OUT_DIR = "data/raw/clean_baseline"

def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    rng = random.Random(42)
    now = datetime.now().replace(microsecond=0)
    
    # Find the clean baseline recipes
    weekday_recipe = next((r for r in RECIPES if r["key"] == "clean_baseline"), None)
    weekend_recipe = next((r for r in RECIPES if r["key"] == "clean_baseline_weekend"), None)
    
    if not weekday_recipe or not weekend_recipe:
        print("Error: Could not find clean_baseline recipes in gen_vendor_neutral_logs.py")
        sys.exit(1)
    
    total_files = 80 # ~80 files * ~38 lines = ~3000 lines
    
    written = []
    for i in range(total_files):
        # 75% weekday, 25% weekend
        if rng.random() < 0.75:
            recipe = weekday_recipe
        else:
            recipe = weekend_recipe
            
        # Spread across the last 14 days to simulate history
        base = now - timedelta(days=14 - (i * (14 / total_files)), hours=rng.uniform(0, 4))
        base = base.replace(microsecond=0)
        
        # Add random padding to introduce variety in lengths (e.g. 5 to 15 background logs)
        pad = rng.randint(5, 15)
        
        text = _gen_scenario(i + 1, recipe, base, rng, pad)
        path = os.path.join(OUT_DIR, f"baseline_{i + 1:03d}_{recipe['key']}.log")
        
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(text)
        written.append((path, recipe["severity"], base))

    print(f"Wrote {len(written)} pure clean baseline files to {OUT_DIR}/")
    print(f"Proceed to perform the cold start by running:")
    print(f"  rm -f ml/model_store/*")
    print(f"  python pipeline.py --dry-run --log-file {OUT_DIR} --input-mode synthetic")

if __name__ == "__main__":
    main()
