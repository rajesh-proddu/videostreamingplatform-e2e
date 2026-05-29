package dataservice_io

import "testing"

// TestGRPC_StreamingPaths is intentionally a Skip. The gRPC client for the
// data-service lives in `github.com/yourusername/videostreamingplatform/dataservice/pb`,
// which is in a separate Go module (`github.com/yourusername/videostreamingplatform`).
//
// Pulling it in here would require:
//   - a `replace` directive in this module's go.mod pointing at the sibling repo,
//   - adding google.golang.org/grpc and google.golang.org/protobuf dependencies,
//   - and the proto's go_package option is set to a non-import-style path
//     (`videostreamingplatform/dataservice/pb`), so we can't simply `go get` it.
//
// Additionally, the existing proto exposes only unary RPCs (InitiateUpload,
// UploadChunk, GetUploadProgress, CompleteUpload, ListUploads) — there is no
// server-streaming download RPC to compare against HTTP, so even with the
// client wired the comparison the task asks for (streaming-download vs HTTP)
// would not be apples-to-apples.
//
// Per the task's "skip with a clear message" fallback, we skip.
func TestGRPC_StreamingPaths(t *testing.T) {
	t.Skip("gRPC client not directly importable (cross-module replace + new deps required); " +
		"proto has only unary RPCs, no streaming download to compare. See package README.")
}
