public class RequestBody {
    private String inputs;
    private Parameters parameters;

    public RequestBody(String inputs, Parameters parameters) {
        this.inputs = inputs;
        this.parameters = parameters;
    }
    public String getInputs() {
        return inputs;
    }

}
