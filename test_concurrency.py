#!/usr/bin/env python3
"""
Test LiteLLM concurrency limits for cli-gpt-5.2
"""

import asyncio
import os
import time
from dotenv import load_dotenv
from openai import AsyncOpenAI

load_dotenv()

client = AsyncOpenAI(
    api_key=os.getenv("LITELLM_API_KEY"),
    base_url=os.getenv("LITELLM_ENDPOINT"),
)

async def single_request(request_id: int, model: str = "cli-gpt-5.2") -> dict:
    """Make a single API request and return timing info."""
    start = time.time()
    try:
        response = await client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": f"Say 'Request {request_id} OK' in exactly 3 words."}],
            max_tokens=20,
        )
        elapsed = time.time() - start
        return {
            "id": request_id,
            "success": True,
            "elapsed": elapsed,
            "response": response.choices[0].message.content[:50] if response.choices else "",
        }
    except Exception as e:
        elapsed = time.time() - start
        return {
            "id": request_id,
            "success": False,
            "elapsed": elapsed,
            "error": str(e)[:100],
        }


async def test_concurrency(concurrency: int, total_requests: int = None) -> dict:
    """Test a specific concurrency level."""
    if total_requests is None:
        total_requests = concurrency

    print(f"\n{'='*60}")
    print(f"Testing concurrency: {concurrency} (total requests: {total_requests})")
    print(f"{'='*60}")

    start = time.time()

    # Create semaphore to limit concurrency
    semaphore = asyncio.Semaphore(concurrency)

    async def bounded_request(request_id):
        async with semaphore:
            return await single_request(request_id)

    # Run all requests with bounded concurrency
    tasks = [bounded_request(i) for i in range(total_requests)]
    results = await asyncio.gather(*tasks)

    total_time = time.time() - start

    # Analyze results
    successes = [r for r in results if r["success"]]
    failures = [r for r in results if not r["success"]]

    success_rate = len(successes) / len(results) * 100 if results else 0
    avg_latency = sum(r["elapsed"] for r in successes) / len(successes) if successes else 0
    throughput = len(successes) / total_time if total_time > 0 else 0

    print(f"\nResults:")
    print(f"  Total requests: {len(results)}")
    print(f"  Successful: {len(successes)} ({success_rate:.1f}%)")
    print(f"  Failed: {len(failures)}")
    print(f"  Total time: {total_time:.2f}s")
    print(f"  Avg latency: {avg_latency:.2f}s")
    print(f"  Throughput: {throughput:.2f} req/s")

    if failures:
        print(f"\nSample errors:")
        for f in failures[:3]:
            print(f"  - Request {f['id']}: {f.get('error', 'Unknown')}")

    return {
        "concurrency": concurrency,
        "total_requests": total_requests,
        "successes": len(successes),
        "failures": len(failures),
        "success_rate": success_rate,
        "total_time": total_time,
        "avg_latency": avg_latency,
        "throughput": throughput,
    }


async def main():
    print("=" * 60)
    print("LiteLLM Concurrency Test for cli-gpt-5.2")
    print("=" * 60)
    print(f"Endpoint: {os.getenv('LITELLM_ENDPOINT')}")

    # Test increasing concurrency levels
    concurrency_levels = [1, 2, 5, 10, 15, 20, 30, 50]
    results = []

    for concurrency in concurrency_levels:
        # For higher concurrency, send more requests to get stable metrics
        total_requests = max(concurrency, 10)
        result = await test_concurrency(concurrency, total_requests)
        results.append(result)

        # Stop if we hit too many failures
        if result["success_rate"] < 80:
            print(f"\n⚠️ Success rate dropped below 80% at concurrency {concurrency}")
            break

        # Brief pause between tests
        await asyncio.sleep(2)

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"{'Concurrency':<12} {'Success%':<10} {'Throughput':<12} {'Avg Latency':<12}")
    print("-" * 60)
    for r in results:
        print(f"{r['concurrency']:<12} {r['success_rate']:<10.1f} {r['throughput']:<12.2f} {r['avg_latency']:<12.2f}")

    # Find optimal concurrency
    valid_results = [r for r in results if r["success_rate"] >= 95]
    if valid_results:
        best = max(valid_results, key=lambda x: x["throughput"])
        print(f"\n✅ Recommended concurrency: {best['concurrency']} (throughput: {best['throughput']:.2f} req/s)")
    else:
        print("\n⚠️ No concurrency level achieved 95%+ success rate")


if __name__ == "__main__":
    asyncio.run(main())
