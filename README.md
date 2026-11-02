# ComfyUI Monitor Link

A companion background extension for **ComfyUI** that bridges real-time execution events, telemetry, and media assets directly to the **ComfyUI Monitor** Corsair iCUE Widget.

## Features

- **Secondary WebSocket Channel (`/ws/monitor`)**: Streams live execution events (active node, step progress, preview latents, completion, queue depth) without ever colliding with or disconnecting your main web browser GUI.
- **REST Status Endpoint (`/api/execution_status`)**: Provides instantaneous execution state and monitor connection metrics.
- **On-Demand WebM Transcoder (`/api/video_to_webm`)**: High-performance hardware/software VP9/VP8 transcoding for non-native video outputs (e.g. MP4, MOV) to ensure zero-latency inline playback in embedded WebView and iCUE widgets.
- **Zero Canvas Clutter**: Registers zero workflow nodes, requiring no modifications to your existing ComfyUI workflows.

## Installation

Clone this repo into custom_nodes folder
```
ComfyUI/custom_nodes/comfyui-monitor-link/
```
Restart ComfyUI. The extension will automatically activate.
