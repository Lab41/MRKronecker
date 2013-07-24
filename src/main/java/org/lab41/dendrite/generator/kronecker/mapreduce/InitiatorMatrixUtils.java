package org.lab41.dendrite.generator.kronecker.mapreduce;

public class InitiatorMatrixUtils {
    /**
     * Finds the sum of the elements of the given initiator matrix.
     * @param initiatorMatrix
     * @return 
     */
    public static double calculateMatrixSum(double[][] initiatorMatrix)
    {
        double sum = 0d;
        for(int i=0 ; i < initiatorMatrix.length; i++)
        {
            for(int k=0; k < initiatorMatrix[i].length; k++)
            {
                sum += initiatorMatrix[i][k];
            }
        }
        return sum;
    }


    /**
     * Expects the probability matrix as a comma separated string " t11, t12, t21, t22"
     *
     * @param strProbabilityMartix
     * @return
     */
     public static double[][] parseInitiatorMatrix(String strProbabilityMartix) {
        String[] splitMatrix = strProbabilityMartix.split(",");
        double[][] probabilityMatrix = new double[2][2];

        if (splitMatrix.length == 4) {
            probabilityMatrix[0][0] = Double.parseDouble(splitMatrix[0]);
            probabilityMatrix[0][1] = Double.parseDouble(splitMatrix[1]);
            probabilityMatrix[1][0] = Double.parseDouble(splitMatrix[2]);
            probabilityMatrix[1][1] = Double.parseDouble(splitMatrix[3]);

        } else {
            throw new RuntimeException("the probablity matrix is not valid");
        }

        // probablity_matrix = probabilityMatrix;
        return probabilityMatrix;
    }
}