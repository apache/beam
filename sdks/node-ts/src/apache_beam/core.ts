class PValue {
    constructor() {
    }

    apply(transform: PTransform): PValue {
        return transform.expand(this);
    }

    map(callable): PValue {
        return this.apply(new ParDo(callable));
    }

}

class Pipeline extends PValue {

}

class PCollection extends PValue {

}

class PTransform {
    expand(input: PValue): PValue {
        throw new Error('Method expand has not been implemented.');
    }
}

class ParDo extends PTransform {
    private doFn;
    constructor(callableOrDoFn) {
        super()
        this.doFn = callableOrDoFn;
    }
}

class DoFn {
    
}