"use client";

import React, { useState, useEffect } from "react";
import { Activity, Server, AlertTriangle, CheckCircle2 } from "lucide-react";
import ClusterVisualizer from "@/components/ClusterVisualizer";

export default function Dashboard() {
	const [metrics, setMetrics] = useState({
		activeNodes: 0,
		cpuLoad: 0,
		tasksProcessed: 0,
		dlqSize: 0,
	});

	const [isLive, setIsLive] = useState(false);

	useEffect(() => {
		const fetchMetrics = async () => {
			try {
				const res = await fetch("/api/metrics");
				if (res.ok) {
					const data = await res.json();
					setMetrics(data);
					setIsLive(true);
				} else {
					setIsLive(false);
				}
			} catch (error) {
				console.error("Telemetry disconnected:", error);
				setIsLive(false);
			}
		};

		fetchMetrics();

		const interval = setInterval(fetchMetrics, 3000);
		return () => clearInterval(interval);
	}, []);

	return (
		<div className="min-h-screen bg-slate-50 text-slate-900 font-sans p-8">
			<div className="max-w-6xl mx-auto">
				<header className="mb-10 flex justify-between items-end">
					<div>
						<h1 className="text-3xl font-bold tracking-tight text-slate-900">
							Cortex-MQ
						</h1>
						<p className="text-sm text-slate-500 mt-1">
							Distributed AI Orchestration & Telemetry
						</p>
					</div>

					<div
						className={`flex items-center space-x-2 px-3 py-1.5 rounded-full border shadow-sm ${isLive ? "bg-green-50 border-green-200" : "bg-red-50 border-red-200"}`}
					>
						<span className="relative flex h-2.5 w-2.5">
							<span
								className={`animate-ping absolute inline-flex h-full w-full rounded-full opacity-75 ${isLive ? "bg-green-400" : "bg-red-400"}`}
							></span>
							<span
								className={`relative inline-flex rounded-full h-2.5 w-2.5 ${isLive ? "bg-green-500" : "bg-red-500"}`}
							></span>
						</span>
						<span
							className={`text-xs font-bold uppercase tracking-wider ${isLive ? "text-green-700" : "text-red-700"}`}
						>
							{isLive ? "System Operational" : "Broker Disconnected"}
						</span>
					</div>
				</header>

				<div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
					<div className="bg-white rounded-2xl p-6 border border-slate-200 shadow-sm flex flex-col justify-between transition-all hover:shadow-md">
						<div className="flex justify-between items-start">
							<p className="text-sm font-semibold text-slate-500">
								Active Swarm Nodes
							</p>
							<div className="p-2 bg-blue-50 rounded-lg text-blue-600">
								<Server size={20} />
							</div>
						</div>
						<div className="mt-4">
							<h3 className="text-4xl font-black text-slate-900">
								{metrics.activeNodes}
							</h3>
							<p className="text-xs font-medium text-slate-400 mt-1">
								Connected via Consistent Hash Ring
							</p>
						</div>
					</div>

					<div className="bg-white rounded-2xl p-6 border border-slate-200 shadow-sm flex flex-col justify-between transition-all hover:shadow-md">
						<div className="flex justify-between items-start">
							<p className="text-sm font-semibold text-slate-500">
								Average CPU Load
							</p>
							<div className="p-2 bg-indigo-50 rounded-lg text-indigo-600">
								<Activity size={20} />
							</div>
						</div>
						<div className="mt-4">
							<h3 className="text-4xl font-black text-slate-900">
								{metrics.cpuLoad}%
							</h3>
							<p className="text-xs font-medium text-slate-400 mt-1">
								Aggregated cluster telemetry
							</p>
						</div>
					</div>

					<div className="bg-white rounded-2xl p-6 border border-slate-200 shadow-sm flex flex-col justify-between transition-all hover:shadow-md">
						<div className="flex justify-between items-start">
							<p className="text-sm font-semibold text-slate-500">
								Tasks Completed
							</p>
							<div className="p-2 bg-emerald-50 rounded-lg text-emerald-600">
								<CheckCircle2 size={20} />
							</div>
						</div>
						<div className="mt-4">
							<h3 className="text-4xl font-black text-slate-900">
								{metrics.tasksProcessed}
							</h3>
							<p className="text-xs font-medium text-slate-400 mt-1">
								Successfully resolved via Agents
							</p>
						</div>
					</div>

					<div
						className={`bg-white rounded-2xl p-6 border shadow-sm flex flex-col justify-between transition-all hover:shadow-md ${metrics.dlqSize > 0 ? "border-red-300 ring-2 ring-red-50 bg-red-50/20" : "border-slate-200"}`}
					>
						<div className="flex justify-between items-start">
							<p
								className={`text-sm font-semibold ${metrics.dlqSize > 0 ? "text-red-700" : "text-slate-500"}`}
							>
								Dead Letter Queue
							</p>
							<div
								className={`p-2 rounded-lg ${metrics.dlqSize > 0 ? "bg-red-100 text-red-700" : "bg-slate-50 text-slate-400"}`}
							>
								<AlertTriangle size={20} />
							</div>
						</div>
						<div className="mt-4">
							<h3
								className={`text-4xl font-black ${metrics.dlqSize > 0 ? "text-red-700" : "text-slate-900"}`}
							>
								{metrics.dlqSize}
							</h3>
							<p
								className={`text-xs font-medium mt-1 ${metrics.dlqSize > 0 ? "text-red-500" : "text-slate-400"}`}
							>
								{metrics.dlqSize > 0
									? "Critical: Tasks require manual intervention"
									: "Queue is currently clear"}
							</p>
						</div>
					</div>
				</div>

				<div className="mt-6">
					<ClusterVisualizer activeNodes={metrics.activeNodes} />
				</div>
			</div>
		</div>
	);
}
