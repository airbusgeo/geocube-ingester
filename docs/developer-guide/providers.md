# Providers

NB: This documentation is for developer that want to implement a new Image Provider. For documentation on how to use the image providers, see [User-Guide/Providers](#user-guide/providers.md).


## Add a new provider

1. Add configuration parameters (credentials, endpoint) in `cmd/downloader/main.go` and add the new provider to the list of providers.
2. Implement the new provider in `ìnterface/provider` with methods:

```go
Name() string
Download(ctx context.Context, scene common.Scene, localDir string) error
```

3. Update the documentation [docs/user-guide/providers.md](../user-guide/providers.md)