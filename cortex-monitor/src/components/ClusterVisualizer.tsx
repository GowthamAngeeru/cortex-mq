import React from "react";

export default function ClusterVisualizer({ activeNodes = 0 }) {
	const workers = Array.from({ length: activeNodes });
	const radius = 180; // Radial distance from broker center

	return (
		<div className="w-full flex flex-col items-center justify-center py-12 bg-white rounded-xl border border-gray-100 shadow-sm">
			<div className="text-center mb-12">
				<h3 className="text-lg font-semibold text-gray-800">
					Live Cluster Topology
				</h3>
				<p className="text-sm text-gray-500">
					Real-time gRPC telemetry mapping
				</p>
			</div>

			<div className="relative flex items-center justify-center w-full max-w-4xl h-64">
				{/* Central Broker Node */}
				<div className="absolute z-10 flex flex-col items-center">
					<div className="w-20 h-20 bg-blue-600 rounded-full flex items-center justify-center shadow-lg shadow-blue-500/30 animate-pulse">
						<svg
							className="w-10 h-10 text-white"
							fill="none"
							viewBox="0 0 24 24"
							stroke="currentColor"
						>
							<path
								strokeLinecap="round"
								strokeLinejoin="round"
								strokeWidth={2}
								d="M5 12h14M5 12a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v4a2 2 0 01-2 2M5 12a2 2 0 00-2 2v4a2 2 0 002 2h14a2 2 0 002-2v-4a2 2 0 00-2-2m-2-4h.01M17 16h.01"
							/>
						</svg>
					</div>
					<span className="mt-4 font-mono text-sm font-bold text-gray-700 bg-gray-100 px-3 py-1 rounded-full">
						Cortex-MQ Broker
					</span>
				</div>

				{/* Unified Absolute SVG Overlay for Connection Lines */}
				{workers.length > 0 && (
					<svg
						className="absolute inset-0 w-full h-full pointer-events-none"
						style={{ overflow: "visible" }}
					>
						{workers.map((_, index) => {
							const angle = (Math.PI / (workers.length + 1)) * (index + 1);
							const x = Math.cos(angle) * radius;
							const y = Math.sin(angle) * radius - 40;

							return (
								<line
									key={index}
									x1="50%"
									y1="50%"
									// Use style properties for cross-browser calc usage within SVG geometry
									style={{
										x2: `calc(50% + ${x}px)`,
										y2: `calc(50% + ${y}px)`,
									}}
									stroke="#10B981"
									strokeWidth="2"
									strokeDasharray="4 4"
									className="opacity-40 transition-all duration-700 ease-in-out"
								/>
							);
						})}
					</svg>
				)}

				{/* Worker Nodes Nodes Layer */}
				{workers.length > 0 ? (
					workers.map((_, index) => {
						const angle = (Math.PI / (workers.length + 1)) * (index + 1);
						const x = Math.cos(angle) * radius;
						const y = Math.sin(angle) * radius - 40;

						return (
							<div
								key={index}
								className="absolute flex flex-col items-center transition-all duration-700 ease-in-out"
								style={{ transform: `translate(${x}px, ${y}px)` }}
							>
								<div className="w-12 h-12 bg-emerald-500 rounded-full flex items-center justify-center shadow-md shadow-emerald-500/20 ring-4 ring-emerald-50">
									<svg
										className="w-6 h-6 text-white"
										fill="none"
										viewBox="0 0 24 24"
										stroke="currentColor"
									>
										<path
											strokeLinecap="round"
											strokeLinejoin="round"
											strokeWidth={2}
											d="M13 10V3L4 14h7v7l9-11h-7z"
										/>
									</svg>
								</div>
								<span className="mt-2 text-xs font-semibold text-gray-500">
									Worker-{index + 1}
								</span>
							</div>
						);
					})
				) : (
					<div className="absolute flex flex-col items-center transform translate-y-24 opacity-50">
						<span className="text-sm font-semibold text-red-500">
							Waiting for Swarm Connections...
						</span>
					</div>
				)}
			</div>
		</div>
	);
}
