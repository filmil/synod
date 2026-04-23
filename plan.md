1. **Analyze the bottleneck**: The `fetchGRPCChannels` and `fetchGRPCServers` methods currently append to the `channels` and `servers` slices respectively one by one using `append` in a loop. When `resp.Channel` or `resp.Server` is large, this causes multiple allocations (N+1 memory issue).
2. **Optimize `fetchGRPCChannels`**: Pre-allocate the `channels` slice with the capacity equal to the length of `resp.Channel`.
3. **Optimize `fetchGRPCServers`**: Pre-allocate the `servers` slice with the capacity equal to the length of `resp.Server`.
4. **Pre-commit step**: Run pre-commit instructions, run `bazelisk test //internal/server:server_test --test_arg=-test.bench=. --test_arg=-test.benchmem --test_output=all --cache_test_results=no --test_arg=-test.v` to verify benchmark improvement.
5. **Submit**: Submit the code.
