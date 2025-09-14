import argparse, json, time
from .engine import QueryEngine

def main():
    ap = argparse.ArgumentParser(description="AQP Engine CLI")
    ap.add_argument('--query', required=True, help='SQL-like query')
    ap.add_argument('--method', default='sample', choices=['sample','stream','exact'])
    ap.add_argument('--sample_rate', type=float, default=0.1)
    ap.add_argument('--seed', type=int, default=42)
    ap.add_argument('--stream_k', type=int, default=10000)
    ap.add_argument('--show_exact', action='store_true', help='Also compute exact for comparison')
    args = ap.parse_args()

    eng = QueryEngine()
    out = eng.run(args.query, method=args.method, sample_rate=args.sample_rate, seed=args.seed,
                  streaming_k=args.stream_k, return_exact=args.show_exact)
    print(json.dumps(out, indent=2))

if __name__ == '__main__':
    main()
