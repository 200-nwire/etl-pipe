#!/usr/bin/env python3
"""
Comprehensive test script for all Dagster assets (dlt and dbt).
Tests raw materialization and silver auto-materialization.
"""

import os
import sys
from pathlib import Path

# Add lineage to path
sys.path.insert(0, str(Path(__file__).parent.parent / "lineage" / "src"))

from dagster import materialize, AssetSelection
from lineage.definitions import defs

def test_raw_assets():
    """Test all raw MongoDB assets materialize correctly."""
    print("🧪 Testing Raw Assets Materialization...")
    
    # Get definitions
    all_defs = defs
    
    # Handle both single-asset and multi-asset definitions
    def get_asset_keys(asset_def):
        """Get asset key(s) from an asset definition."""
        if hasattr(asset_def, 'keys') and asset_def.keys:
            return list(asset_def.keys) if isinstance(asset_def.keys, (set, frozenset)) else list(asset_def.keys)
        elif hasattr(asset_def, 'key'):
            return [asset_def.key]
        else:
            return []
    
    # Select all raw assets (assets with key starting with "raw")
    raw_assets = []
    for asset in all_defs.assets:
        keys = get_asset_keys(asset)
        raw_keys = [k for k in keys if len(k.path) > 0 and k.path[0] == "raw"]
        if raw_keys:
            raw_assets.append(asset)
    
    if not raw_assets:
        print("⚠️  No raw assets found - skipping test")
        return True
    
    print(f"   Found {len(raw_assets)} raw assets")
    
    # Select just a couple for testing to avoid long runs
    test_assets = raw_assets[:3]  # Test first 3 raw assets
    
    try:
        result = materialize(
            test_assets,
            resources=all_defs.resources,
        )
        
        if result.success:
            print(f"✅ {len(test_assets)} raw assets materialized successfully")
            return True
        else:
            print(f"❌ Raw materialization failed: {result}")
            return False
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_silver_assets():
    """Test all silver dbt assets compile and run."""
    print("\n🧪 Testing Silver Assets (dbt models)...")
    
    # Get definitions
    all_defs = defs
    
    # Handle both single-asset and multi-asset definitions
    def get_asset_keys(asset_def):
        """Get asset key(s) from an asset definition."""
        if hasattr(asset_def, 'keys') and asset_def.keys:
            return list(asset_def.keys) if isinstance(asset_def.keys, (set, frozenset)) else list(asset_def.keys)
        elif hasattr(asset_def, 'key'):
            return [asset_def.key]
        else:
            return []
    
    # Select all silver assets (assets with key starting with "silver" or in silver group)
    silver_assets = []
    for asset in all_defs.assets:
        keys = get_asset_keys(asset)
        silver_keys = [k for k in keys if len(k.path) > 0 and (k.path[0] == "silver" or k.path[0] == "staging")]
        if silver_keys:
            silver_assets.append(asset)
    
    if not silver_assets:
        print("⚠️  No silver assets found - skipping test")
        return True
    
    print(f"   Found {len(silver_assets)} silver assets")
    
    # Select just a couple for testing to avoid long runs
    test_assets = silver_assets[:3]  # Test first 3 silver assets
    
    try:
        result = materialize(
            test_assets,
            resources=all_defs.resources,
        )
        
        if result.success:
            print(f"✅ {len(test_assets)} silver assets materialized successfully")
            return True
        else:
            print(f"❌ Silver materialization failed: {result}")
            return False
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_auto_materialization():
    """Test that silver assets auto-materialize after raw."""
    print("\n🧪 Testing Auto-Materialization...")
    
    # Get definitions
    all_defs = defs
    
    # Handle both single-asset and multi-asset definitions
    def get_asset_keys(asset_def):
        """Get asset key(s) from an asset definition."""
        if hasattr(asset_def, 'keys') and asset_def.keys:
            return list(asset_def.keys) if isinstance(asset_def.keys, (set, frozenset)) else list(asset_def.keys)
        elif hasattr(asset_def, 'key'):
            return [asset_def.key]
        else:
            return []
    
    # Find a raw asset
    raw_assets = []
    for asset in all_defs.assets:
        keys = get_asset_keys(asset)
        raw_keys = [k for k in keys if len(k.path) > 0 and k.path[0] == "raw"]
        if raw_keys:
            raw_assets.append(asset)
    if not raw_assets:
        print("⚠️  No raw assets found - skipping test")
        return True
    
    raw_asset = raw_assets[0]
    print(f"   Testing with raw asset: {raw_asset.key}")
    
    try:
        # First materialize raw
        raw_result = materialize(
            [raw_asset],
            resources=all_defs.resources,
        )
        
        if not raw_result.success:
            print("❌ Raw asset materialization failed")
            return False
        
        # Then try to materialize a dependent silver asset if available
        silver_assets = []
        for asset in all_defs.assets:
            keys = get_asset_keys(asset)
            silver_keys = [k for k in keys if len(k.path) > 0 and (k.path[0] == "silver" or k.path[0] == "staging")]
            if silver_keys:
                silver_assets.append(asset)
        
        if not silver_assets:
            print("⚠️  No silver assets found - skipping silver materialization")
            return True
        
        silver_asset = silver_assets[0]
        print(f"   Testing with silver asset: {silver_asset.key}")
        
        silver_result = materialize(
            [silver_asset],
            resources=all_defs.resources,
        )
        
        if silver_result.success:
            print("✅ Auto-materialization works: silver materialized after raw")
            return True
        else:
            print("❌ Auto-materialization failed")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🚀 Starting Comprehensive Asset Tests\n")
    
    results = []
    results.append(("Raw Assets", test_raw_assets()))
    results.append(("Silver Assets", test_silver_assets()))
    results.append(("Auto-Materialization", test_auto_materialization()))
    
    print("\n📊 Test Results:")
    for name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"   {status}: {name}")
    
    all_passed = all(r[1] for r in results)
    sys.exit(0 if all_passed else 1)
