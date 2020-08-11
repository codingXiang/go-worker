package go_worker

//BuildKeyPath 組合 etcd key 的路徑
func BuildKeyPath(basePath string, paths ...string) string {
	for _, path := range paths {
		basePath += "/" + path
	}
	return basePath
}
