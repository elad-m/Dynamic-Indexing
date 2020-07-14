package dynamic_index.global_util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static java.lang.System.exit;

/**
 * Encodes and decodes between integers to Length Precoded Varint byte array, as a compression method.
 * Integers encodeable by this method are in the range of [0...2^30-1], since the 2 MSB bits are used for
 * encoding the length by bytes of the encoded integer.
 */
public class LengthPrecodedVarintCodec {

    //=========================  Encoding  =====================================//

    private static final int[] LENGTH_PRECODED_MAXIMA =
            {63, 16383, 4194303, 1073741823};// 2^6-1, 2^14-1, 2^22-1, 2^30-1
    private static final int[] BITWISE_OR_OPERAND_TO_ENCODE_LENGTH_PRECODED_VARINT =
            {0, 16384, 8388608, -1073741824}; // 00|zeros, 2^14, 2^23, 2^31+2^30

    /**
     * Given an integer in the range [0...2^30-1], returns the length-precoded byte array of the integer.
     * Length pre-coding codes the length of the integer value in byte in the first 2 bits of the byte array.
     * This way smaller numbers take less number of bytes for writing.
     * The ranges for conversions and the conversion tools are detailed in the constants above.
     * @param value - an integer value in the range [0...2^30-1] (last 2 MSB are for encoding)
     * @return the value of the integer encoded in length-precoded varint method.
     */
    public static byte[] intToCompressedByteArray(int value){
        byte[] ret = new byte[0];
        if (value <= LENGTH_PRECODED_MAXIMA[0]) {
            ret = toLengthPrecodedVarint(value, 1);
        } else if (value <= LENGTH_PRECODED_MAXIMA[1]) {
            ret = toLengthPrecodedVarint(value, 2);
        } else if (value <= LENGTH_PRECODED_MAXIMA[2]) {
            ret = toLengthPrecodedVarint(value, 3);
        } else if (value <= LENGTH_PRECODED_MAXIMA[3]) {
            ret = toLengthPrecodedVarint(value, 4);
        } else {
            System.err.println("Value too large to be represented in Length Precoded Varint");
            System.exit(8);
        }
        return ret;
    }

    private static byte[] toLengthPrecodedVarint(int input, int numberOfBytes) {
        byte[] resultLenPrecodeVarint;
        if (numberOfBytes == 1) {
            resultLenPrecodeVarint = new byte[]{(byte) input};
        } else if (numberOfBytes == 2) {
            ByteBuffer byteBuffer = ByteBuffer.allocate(Short.BYTES);
            byteBuffer.putShort((short) (input | BITWISE_OR_OPERAND_TO_ENCODE_LENGTH_PRECODED_VARINT[1]));
            resultLenPrecodeVarint = byteBuffer.array();
            assert resultLenPrecodeVarint.length == 2;
        } else if (numberOfBytes == 3) {
            ByteBuffer byteBufferTarget = ByteBuffer.allocate(Short.BYTES + 1);
            ByteBuffer byteBufferInteger = ByteBuffer.allocate(Integer.BYTES);
            byteBufferInteger.putInt(input | BITWISE_OR_OPERAND_TO_ENCODE_LENGTH_PRECODED_VARINT[2]);
            byteBufferTarget.put(byteBufferInteger.get(1));
            byteBufferTarget.put(byteBufferInteger.get(2));
            byteBufferTarget.put(byteBufferInteger.get(3));
            resultLenPrecodeVarint = byteBufferTarget.array();
            assert resultLenPrecodeVarint.length == 3;
        } else if (numberOfBytes == 4) {
            ByteBuffer byteBufferInteger = ByteBuffer.allocate(Integer.BYTES);
            byteBufferInteger.putInt(input | BITWISE_OR_OPERAND_TO_ENCODE_LENGTH_PRECODED_VARINT[3]);
            resultLenPrecodeVarint = byteBufferInteger.array();
            assert resultLenPrecodeVarint.length == 4;
        } else {
            System.err.println("Number bigger than length-precoded varint integer detected");
            resultLenPrecodeVarint = new byte[0];
        }
        return resultLenPrecodeVarint;
    }

    //=========================  Decoding  =====================================//
    // also does the gaps (returns the absolute rids)

    private static final short BITWISE_AND_OPERAND_TO_DECODE_SHORT = -16385; // 101111..
    // next line is from: 0b, 01111111, -1b, -1b
    private static final int BITWISE_AND_OPERAND_TO_DECODE_THREE_BYTES = 8388607;
    private static final int BITWISE_AND_OPERAND_TO_DECODE_INTEGER = 1073741823;
    private static final int AND_OPERAND_FOR_RIGHT_SHIFTING_TRUE_BYTE_VALUE = 255;

    /**
     * Decodes a byte array into a list of integers.
     * Pay attention to gaps, if the argument was encoded with those outside this class as well.
     * @param bytesToDecode -
     * @return - list of integers as decoded by length-precoded varint.
     */
    public static List<Integer> decodeBytesToIntegers(byte[] bytesToDecode) {
        // using length pre-coded varint
        ByteBuffer byteBufferOfRow = ByteBuffer.wrap(bytesToDecode);
        List<Integer> integersInBytesRow = new ArrayList<>();

        int i = 0;
        while (i < bytesToDecode.length) {
            byte someByte = byteBufferOfRow.get(i);
            int numOfBytesRoRead = getNumberOfBytesToRead(someByte);
            switch (numOfBytesRoRead) {
                case 1:
                    integersInBytesRow.add((int) someByte);
                    i++;
                    break;
                case 2:
                    short rid2 = byteBufferOfRow.getShort(i);
                    int debug1 = getDecodedInteger(rid2);
                    integersInBytesRow.add(debug1);
                    i += 2;
                    break;
                case 3:
                    byte[] threeArray = new byte[4];
                    threeArray[0] = 0;
                    threeArray[1] = byteBufferOfRow.get(i);
                    threeArray[2] = byteBufferOfRow.get(i + 1);
                    threeArray[3] = byteBufferOfRow.get(i + 2);
                    int rid3 = ByteBuffer.wrap(threeArray).getInt();
                    int debug2 = getDecodedInteger(rid3, true);
                    integersInBytesRow.add(debug2);
                    i += 3;
                    break;
                case 4:
                    int rid4 = byteBufferOfRow.getInt(i);
                    int debug3 = getDecodedInteger(rid4, false);
                    integersInBytesRow.add(debug3);
                    i += 4;
                    break;
                default:
                    System.err.println("got not between 1-4 bytes to read");
                    break;
            }
        }
        return integersInBytesRow;
    }

    private static int getNumberOfBytesToRead(byte someByte) {
        /* Bitwise operations in java convert up to int anything it gets. To get correct results with this impediment
         *  the constant 255 operand bellow makes all the 1s above the 8 bit of negative byte numbers to zero, so now they appear to be
         * they true self as negative byte, e.g: -128 & 255 = 128. so now 128>>>6 = 2, what we wanted. Credits for the IDE
         * for bringing this to my attention.*/
        int javaBadTypePromotionByProduct = someByte & AND_OPERAND_FOR_RIGHT_SHIFTING_TRUE_BYTE_VALUE;
        int firstTwoBitsValue = javaBadTypePromotionByProduct >>> 6;
        int numOfBytesToRead;
        switch (firstTwoBitsValue) {
            case 0:
                numOfBytesToRead = 1;
                break;
            case 1:
                numOfBytesToRead = 2;
                break;
            case 2:
                numOfBytesToRead = 3;
                break;
            case 3:
                numOfBytesToRead = 4;
                break;
            default:
                System.err.println("two bits extraction failed");
                exit(2);
                numOfBytesToRead = 0;
        }
        return numOfBytesToRead;
    }

    private static int getDecodedInteger(short shortRid) {
        return (shortRid & BITWISE_AND_OPERAND_TO_DECODE_SHORT);
    }

    private static int getDecodedInteger(int intRid, boolean isThree) {
        if (isThree)
            return (intRid & BITWISE_AND_OPERAND_TO_DECODE_THREE_BYTES);
        else
            return (intRid & BITWISE_AND_OPERAND_TO_DECODE_INTEGER);
    }

}
