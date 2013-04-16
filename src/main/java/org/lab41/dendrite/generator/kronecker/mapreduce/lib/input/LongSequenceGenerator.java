package org.lab41.dendrite.generator.kronecker.mapreduce.lib.input;

/**
 * This interface describes a functor that is used by the {@link LongSequenceRecordReader} to generate
 * keys.
 *
 * Classes that implement this method should implement an onto function f: R->R. That is a function that can
 * map any given long to some output value.
 *
 *  This function will be by LongSeThe input to the function will be an long, selected from the interval of longs assigned to a particular
 * LongSequenceInputSplit.
 *
 * @author kramachandran
 */
public interface LongSequenceGenerator {
    public long generate(long input);
}
