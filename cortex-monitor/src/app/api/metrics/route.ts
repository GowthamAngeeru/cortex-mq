import { NextResponse } from "next/server";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import path from "path";

export const dynamic = "force-dynamic";

const PROTO_PATH = path.join(process.cwd(), "proto", "broker.proto");
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
	keepCase: true,
	longs: String,
	enums: String,
	defaults: true,
	oneofs: true,
});

const cortexProto = grpc.loadPackageDefinition(packageDefinition).cortex as any;

let client: any = null;
function getClient() {
	if (!client) {
		client = new cortexProto.BrokerService(
			"127.0.0.1:50051",
			grpc.credentials.createInsecure(),
		);
	}
	return client;
}

export async function GET() {
	return new Promise((resolve) => {
		const grpcClient = getClient();

		grpcClient.GetSystemStatus({}, (err: any, response: any) => {
			if (err) {
				console.error("gRPC Connection Error:", err.message);
				return resolve(
					NextResponse.json({ error: "Broker offline" }, { status: 503 }),
				);
			}

			let totalCpu = 0;
			if (response.nodes && response.nodes.length > 0) {
				response.nodes.forEach((node: any) => (totalCpu += node.cpu_percent));
			}
			const avgCpu =
				response.nodes && response.nodes.length > 0
					? (totalCpu / response.nodes.length).toFixed(1)
					: 0;

			const liveData = {
				activeNodes: response.active_nodes || 0,
				cpuLoad: parseFloat(avgCpu as string),
				tasksProcessed: response.total_completed || 0,
				dlqSize: response.dlq_size || 0,
			};

			resolve(
				NextResponse.json(liveData, {
					headers: {
						"Cache-Control":
							"no-store, no-cache, must-revalidate, proxy-revalidate",
						Pragma: "no-cache",
						Expires: "0",
					},
				}),
			);
		});
	});
}
