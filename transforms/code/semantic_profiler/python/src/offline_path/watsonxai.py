from ibm_watsonx_ai.metanames import GenTextParamsMetaNames as GenParams
from ibm_watsonx_ai.foundation_models import ModelInference
from ibm_watsonx_ai import Credentials




def generateResponseWatsonx(api_key, api_endpoint, model_id, project_id, input_template):
    credentials = Credentials(api_key=api_key, url=api_endpoint)
    parameters = {
        GenParams.DECODING_METHOD: "greedy",
        GenParams.MAX_NEW_TOKENS: 100,
        GenParams.STOP_SEQUENCES: ["<end>"]
    }
    model = ModelInference(
        model_id=model_id, 
        params=parameters, 
        credentials=credentials,
        project_id=project_id)
    response = model.generate_text(input_template)
    return response
    



