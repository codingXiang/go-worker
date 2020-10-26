package delivery

//go:generate protoc -I grpc/pb grpc/pb/grpc.proto --go_out=plugins=grpc:grpc/pb
//GRPCHttpHandler grpc 流量 handler
type GRPCHttpHandler interface {
	CreateTask()
}
