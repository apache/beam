# TensorRT 10.x RunInference Container

This Docker container provides a GPU-enabled environment for running Apache Beam
pipelines with TensorRT 10.x inference using the new Tensor API.

## Base Image

- **Base**: `nvcr.io/nvidia/tensorrt:25.01-py3`
- **Python**: 3.10
- **TensorRT**: 10.x (included in base image)
- **CUDA**: 12.x (included in base image)

## Dependencies

The container includes:
- Apache Beam 2.67.0 with GCP support
- TensorRT 10.x (from base image)
- CUDA Python 12.8
- PyTorch and TorchVision
- Transformers 4.18.0+
- OpenCV, Pillow, PyMuPDF for image/document processing
- NumPy 2.0.1

## Usage

This container is designed for Dataflow jobs that require GPU acceleration with
TensorRT 10.x. It supports the new `TensorRTEngineHandlerNumPy` handler from
`apache_beam.ml.inference.trt_handler_numpy_compact`.

## Building

```bash
docker build -f tensor_rt_10x.dockerfile -t tensorrt-10x-beam .
```

## GPU Requirements

- NVIDIA GPU with CUDA support
- Compatible with Dataflow GPU workers (e.g., `nvidia-tesla-t4`)
