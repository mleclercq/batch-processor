package com.foo;

import java.io.Serializable;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * A wrapper of either of two values. An `Either` object contains either a value
 * of type `L` in which case it is known as a "left" or a value of type `R` in
 * which case it is known as a "right".
 *
 * This type can be used to represent two possible outcomes of an operation. By
 * convention the right type is considered as the "normal" result while the left
 * type is considered as the "exceptional" (or "abnormal") result.
 *
 * The {@link #right(Object)} constructor and the {@link #flatMap(Function)}
 * method allows to use this class as a monad.
 *
 * @param <L> The type contained by this object, if this object is a "left".
 * @param <R> The type contained by this object, if this object is a "right".
 */
public abstract class Either<L, R>
        implements Serializable {

    /**
     * Returns a new `Either` that contains the given "left" value.
     *
     * @param value the left value to wrap.
     * @param <L>   the left type of the returned `Either`.
     * @param <R>   the right type of the returned `Either`.
     * @return a "left" `Either`.
     */
    public static <L, R> Either<L, R> left(final L value) {
        return castRight(new Left<>(value));
    }

    /**
     * Returns a new `Either` that contains the given "right" value.
     *
     * @param value the right value to wrap.
     * @param <L>   the left type of the returned `Either`.
     * @param <R>   the right type of the returned `Either`.
     * @return a "right" `Either`.
     */
    public static <L, R> Either<L, R> right(final R value) {
        return castLeft(new Right<>(value));
    }

    @SuppressWarnings("unchecked")
    private static <L, U> Either<L, U> castRight(final Either<L, ?> left) {
        assert left.isLeft();
        return (Either<L, U>) left;
    }

    @SuppressWarnings("unchecked")
    private static <U, R> Either<U, R> castLeft(final Either<?, R> right) {
        assert right.isRight();
        return (Either<U, R>) right;
    }

    /**
     * Performs a covariant type conversion of the given `Either`.
     *
     * The `Either` type is effectively covariant on both `L` and `R`. This
     * method allows to perform covariant type conversion without having the
     * Java compiler complaining about unchecked casts.
     *
     * @param either an `Either`.
     * @param <L>    the left type of the given `Either`.
     * @param <R>    the right type of the given `Either`.
     * @param <T>    the left type of the returned `Either`. This type must be a
     *               super-type of `L`.
     * @param <U>    the right type of the returned `Either`. This type must be
     *               a super-type of `R`.
     * @return the given `Either`.
     */
    @SuppressWarnings("unchecked")
    public static <L extends T, R extends U, T, U> Either<T, U> covarCast(final Either<L, R> either) {
        return (Either<T, U>) either;
    }

    public static <L, R> Consumer<Either<L, R>> consumer(
            final Consumer<? super L> leftConsumer,
            final Consumer<? super R> rightConsumer) {
        return v -> {
            if (v.isLeft()) {
                leftConsumer.accept(v.getLeft());
            } else {
                rightConsumer.accept(v.getRight());
            }
        };
    }

    private Either() {
    }

    /**
     * Returns `true` if this `Either` contains a value of type `L`.
     */
    public abstract boolean isLeft();

    /**
     * Returns `true` if this `Either` contains a value of type `R`.
     */
    public final boolean isRight() {
        return !isLeft();
    }

    /**
     * Returns the left value contained by this `Either`.
     *
     * @throws IllegalStateException if this `Either` is a "right".
     */
    public abstract L getLeft();

    /**
     * Returns the right value contained by this `Either`.
     *
     * @throws IllegalStateException if this `Either` is a "left".
     */
    public abstract R getRight();

    /**
     * If this `Either` is a "left", applies the given function; otherwise,
     * returns this `Either`.
     *
     * @param mapFunction the function to apply on the left value.
     * @param <U>         the left type of the returned `Either`.
     * @return this `Either` if it is a "right". If this `Either` is a "left",
     * returns a new "left" `Either` wrapping the result of the given function
     * applied on the "left" value contained by this `Either`.
     */
    public final <U> Either<U, R> mapLeft(
            final Function<? super L, ? extends U> mapFunction) {
        return isLeft() ?
               left(mapFunction.apply(getLeft())) :
               castLeft(this);
    }

    /**
     * If this `Either` is a "right", applies the given function; otherwise,
     * returns this `Either`.
     *
     * @param mapFunction the function to apply on the right value.
     * @param <U>         the right type of the returned `Either`.
     * @return this `Either` if it is a "left". If this `Either` is a "right",
     * returns a new "right" `Either` wrapping the result of the given function
     * applied on the "right" value contained by this `Either`.
     */
    public final <U> Either<L, U> mapRight(
            final Function<? super R, ? extends U> mapFunction) {
        return isLeft() ?
               castRight(this) :
               right(mapFunction.apply(getRight()));
    }

    /**
     * Maps this `Either` using either of the two given functions.
     *
     * @param leftMap  the function to apply if this `Either` is a "left".
     * @param rightMap the function to apply if this `Either` is a "right".
     * @param <T>      the left type of the returned `Either`.
     * @param <U>      the right type of the returned `Either`.
     * @return if this `Either` is a "right", returns a new "right" `Either`
     * wrapping the result of the given `rightMap` function applied on the
     * "right" value contained by this `Either`. If this `Either` is a "left",
     * returns a new "left" `Either` wrapping the result of the given `leftMap`
     * function applied on the "left" value contained by this `Either`.
     */
    public final <T, U> Either<T, U> mapEither(
            final Function<? super L, ? extends T> leftMap,
            final Function<? super R, ? extends U> rightMap) {
        return isLeft() ?
               left(leftMap.apply(getLeft())) :
               right(rightMap.apply(getRight()));
    }

    /**
     * Right flat-mapping. If this `Either` is a left, returns this object.
     * Otherwise returns result of the given function applied on the right value
     * contained by this object.
     *
     * @param flatMapFunction The function to apply on the right value.
     * @param <T>             The type of left returned by the function. This
     *                        type must be sub-type of `L`.
     * @param <U>             The type of right returned by this method.
     * @param <V>             The type of right returned by the given function.
     *                        This type must be q sub-type of `U`.
     * @return this object if it is a left or the result of the given function
     * applied on the right value contained by this object.
     */
    public final <T extends L, U, V extends U> Either<L, U> flatMap(
            final Function<? super R, Either<T, V>> flatMapFunction) {
        return isLeft() ?
               castRight(this) :
               covarCast(flatMapFunction.apply(getRight()));
    }

    /**
     * Reduces this `Either` to a single value by applying either of the two
     * given functions.
     *
     * @param leftReduce  the function to apply if this `Either` is a "left".
     * @param rightReduce the function to apply if this `Either` is a "right".
     * @param <U>         the type returned by this method.
     * @return if this `Either` is a "right", returns the result of the given
     * `rightReduce` function applied on the "right" value contained by this
     * `Either`. If this `Either` is a "left", returns the result of the given
     * `leftReducee` function applied on the "left" value contained by this
     * `Either`.
     */
    public final <U> U reduce(
            final Function<? super L, ? extends U> leftReduce,
            final Function<? super R, ? extends U> rightReduce) {
        return isLeft() ?
               leftReduce.apply(getLeft()) :
               rightReduce.apply(getRight());
    }

    private static final class Left<L>
            extends Either<L, Object> {

        private final L value;

        private Left(final L value) {
            this.value = value;
        }

        @Override
        public boolean isLeft() {
            return true;
        }

        @Override
        public L getLeft() {
            return value;
        }

        @Override
        public Object getRight() {
            throw new IllegalStateException();
        }

        @Override
        public int hashCode() {
            return 11 * Objects.hashCode(value);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            final Left<?> that = (Left<?>) o;
            return Objects.equals(this.value, that.value);
        }

        @Override
        public String toString() {
            return String.format("Left{%s}", value);
        }
    }

    private static final class Right<R>
            extends Either<Object, R> {

        private final R value;

        private Right(final R value) {
            this.value = value;
        }

        @Override
        public boolean isLeft() {
            return false;
        }

        @Override
        public Object getLeft() {
            throw new IllegalStateException();
        }

        @Override
        public R getRight() {
            return value;
        }

        @Override
        public int hashCode() {
            return 13 * Objects.hashCode(value);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            final Right<?> that = (Right<?>) o;
            return Objects.equals(this.value, that.value);
        }

        @Override
        public String toString() {
            return String.format("Right{%s}", value);
        }
    }

    /**
     * Helper class to use {@link Stream} of {@link Either} as monad.
     */
    public static final class StreamMonad {

        /**
         * Performs a covariant type conversion of the given {@link Stream}
         * of {@link Either}.
         *
         * A `Stream<Either<L, R>>` is effectively covariant on both `L` and
         * `R`. This method allows to perform covariant type conversion
         * without having the Java compiler complaining about unchecked casts.
         *
         * @param stream an `Either`.
         * @param <L>    the left type of the given {@link Stream} of
         *               {@link Either}.
         * @param <R>    the right type of the given {@link Stream} of
         *               {@link Either}.
         * @param <T>    the left type of the returned {@link Stream} of
         *               {@link Either}. This type must be a super-type of `L`.
         * @param <U>    the right type of the returned {@link Stream} of
         *               {@link Either}. This type must be a super-type of `R`.
         * @return the given `Either`.
         */
        @SuppressWarnings("unchecked")
        public static <T, U, L extends T, R extends U> Stream<Either<T, U>> covarCast(
                final Stream<Either<L, R>> stream) {
            return (Stream<Either<T, U>>) ((Stream) stream);
        }

        /**
         * Flat-map the "right" values contained in the given {@link Stream}.
         * "left" values are left unchanged.
         *
         * @param stream          the {@link Stream} to flat-map.
         * @param flatMapFunction the function applied on "right" values
         *                        contained in the given stream.
         * @param <L>             the left type of the {@link Either} in the
         *                        given stream.
         * @param <R>             the right type of the {@link Either} in the
         *                        given stream.
         * @param <U>             the right type of the {@link Either} in the
         *                        returned stream.
         * @param <T>             the left type returned by the given function.
         *                        This type must be a sub-type of `L`.
         * @param <V>             the right type returned by the given function.
         *                        This type must be a sub-type of `U`.
         * @return a {@link Stream} where "right" values of the given stream
         * have been flat-mapped with the given function.
         */
        public static <L, R, U, T extends L, V extends U> Stream<Either<L, U>> flatMap(
                final Stream<Either<L, R>> stream,
                final Function<? super R, Stream<Either<T, V>>> flatMapFunction) {
            return stream.flatMap(e -> e.isLeft() ?
                                       Stream.of(castRight(e)) :
                                       covarCast(flatMapFunction.apply(
                                               e.getRight())));
        }

        public static <L, R, U> Stream<Either<L, U>> mapRight(
                final Stream<Either<L, R>> stream,
                final Function<? super R, ? extends U> mapFunction) {
            return stream.map(e -> e.mapRight(mapFunction));
        }

        public static <L, R, U> Stream<Either<U, R>> mapLeft(
                final Stream<Either<L, R>> stream,
                final Function<? super L, ? extends U> mapFunction) {
            return stream.map(e -> e.mapLeft(mapFunction));
        }
    }
}
