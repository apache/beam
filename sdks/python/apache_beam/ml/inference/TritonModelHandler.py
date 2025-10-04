from typing import Sequence, Dict, Any, Iterable, Optional
import logging
import json
import tritonserver
from  tritonserver import Model
from apache_beam.ml.inference.base import ModelHandler, PredictionResult


class TritonModelHandler(ModelHandler[str,
                                     PredictionResult,
                                     Model]):
    
    def __init__(
        self,
        model_repository: str = "/workspace/models",
        model_name: str = "doc_tagging"
    ):
        """
        
        """
        
        self._model_repository = model_repository
        self._model_name = model_name
    
    def load_model(self) -> Model:
        """Loads and initializes a model for processing."""
        server = tritonserver.Server(model_repository=self._model_repository)
        server.start()
        model = server.model(self._model_name)
        return model

    def run_inference(
        self,
        batch: Sequence[str],
        model: Model,
        inference_args: Optional[Dict[str, Any]] = None
    ) -> Iterable[PredictionResult]:
        """Runs inferences on a batch of text strings.
        
        Args:
            batch: A sequence of examples as text strings.
            model: Model returned by Tritonserver.
            inference_args: Any additional arguments for an inference.
        
        Returns:
            An Iterable of type PredictionResult.
        """
        # Loop each text string, and use a tuple to store the inference results.
        logging.info(f"{type(batch)}")
        responses = model.infer(inputs={"string_input":batch})
        for response in responses:
            predictions = [json.loads(answer) for answer in response.outputs["string_output"].to_string_array().tolist()]
        return [PredictionResult(x, y) for x, y in zip(batch, predictions)]

