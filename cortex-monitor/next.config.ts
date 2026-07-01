import type { NextConfig } from "next";

const nextConfig: NextConfig = {
	serverExternalPackages: ["@grpc/grpc-js", "@grpc/proto-loader"],
	output: "standalone",
};

export default nextConfig;
