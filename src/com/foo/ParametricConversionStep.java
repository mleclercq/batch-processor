package com.foo;

import java.util.stream.Stream;

public abstract class ParametricConversionStep<T, E, P>
        implements ConversionStep<T, E> {

    private transient P parameter;

    protected abstract P buildParameter();

    protected abstract Stream<Either<E, T>> apply(T t, P param);

    @Override
    public Stream<Either<E, T>> apply(final T t) {
        if (parameter == null) {
            synchronized (this) {
                if (parameter == null) {
                    parameter = buildParameter();
                }
            }
        }
        return apply(t, parameter);
    }
}
