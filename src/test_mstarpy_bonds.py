import mstarpy as ms
import json

def test_bond_etf():
    isin = "IE00B1FZS681" # iShares EUR Govt Bond 3-5yr
    try:
        f = ms.Funds(isin)
        print(f"Name: {f.name}")
        
        print("\n--- Fixed Income Style ---")
        try:
            fis = f.fixedIncomeStyle()
            with open("mstar_test_output.json", "w") as jf:
                json.dump(fis, jf, indent=4)
            print("Output saved to mstar_test_output.json")
        except Exception as e:
            print(f"fixedIncomeStyle Error: {e}")

        print("\n--- Credit Quality ---")
        try:
            print(json.dumps(f.creditQuality(), indent=4))
        except Exception as e:
            print(f"creditQuality Error: {e}")

        print("\n--- Methods in Funds class ---")
        methods = [m for m in dir(f) if not m.startswith("_")]
        print(methods)
        
    except Exception as e:
        print(f"General Error: {e}")

if __name__ == "__main__":
    test_bond_etf()
