package org.lab41.dendrite.generator.kronecker.mapreduce.lib.input;

/**
 * This is the identity sequence generator. It simply returns the input.
 * Useful for creating a simple
 *
 *
 * @author kramachandran
 */
public class IdentityLongSequenceGenerator implements LongSequenceGenerator {
    @Override
    public long generate(long input) {
       return input;
    }
}
