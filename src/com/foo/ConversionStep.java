package com.foo;

import java.io.Serializable;
import java.util.function.Function;
import java.util.stream.Stream;

public interface ConversionStep<T, E>
        extends Function<T, Stream<Either<E, T>>>, Serializable {

    Stream getName();

    static <T, E> Stream<Either<E, T>> wrapError(final E error) {
        return Stream.of(Either.left(error));
    }

    static <T, E> Stream<Either<E, T>> wrapValue(final T value) {
        return Stream.of(Either.right(value));
    }
}
