package dynamic_index.index_structure;

import dynamic_index.global_tools.MiscTools;

import java.nio.ByteBuffer;

public class ReviewMetaData implements Comparable<ReviewMetaData>{

        public static final int LENGTH_OF_PID = 10;

        private final int rid;
        private final String pid; // literal
        private final short helpN;
        private final short helpD;
        private final byte score;
        private final short reviewLength;

        public static final int sizeOfBytesArray = MiscTools.INTEGER_SIZE + LENGTH_OF_PID +
                3 * Short.BYTES + Byte.BYTES;

        public ReviewMetaData(String[] dataArray){
            assertDataArray(dataArray);
            this.rid = Integer.parseInt(dataArray[0]);
            this.pid = dataArray[1];
            assert pid.length() == LENGTH_OF_PID;
            this.helpN = (short)(Integer.parseInt(dataArray[2]));
            this.helpD = (short)(Integer.parseInt(dataArray[3]));
            this.score = (byte)(Integer.parseInt(dataArray[4]));
            this.reviewLength = (short)(Integer.parseInt(dataArray[5]));
        }


        private void assertDataArray(String[] dataArray) {
            assert (Integer.parseInt(dataArray[2])) <= Short.MAX_VALUE;
            assert (Integer.parseInt(dataArray[3])) <= Short.MAX_VALUE;
            assert (Integer.parseInt(dataArray[4])) <= Byte.MAX_VALUE;
            assert (Integer.parseInt(dataArray[5])) <= Short.MAX_VALUE;
        }

        public ReviewMetaData(byte[] row){
            assert row.length == sizeOfBytesArray;
            ByteBuffer rowBuffer = ByteBuffer.wrap(row);
            rid = rowBuffer.getInt();
            byte[] pidBytes = new byte[LENGTH_OF_PID];
            rowBuffer.get(pidBytes);
            pid = new String(pidBytes);
            score = rowBuffer.get();
            helpN = rowBuffer.getShort();
            helpD = rowBuffer.getShort();
            reviewLength = rowBuffer.getShort();
        }

        public int getRid(){
            return rid;
        }

        public int getScore(){
            return score;
        }

        public int getHelpfulnessNumerator(){
            return helpN;
        }

        public int getHelpfulnessDenominator(){
            return helpD;
        }

        public int getReviewLength(){
            return reviewLength;
        }

        public String getPid(){
            return pid;
        }


        public byte[] asByteArray(){
            ByteBuffer rowBuffer = ByteBuffer.allocate(sizeOfBytesArray);
            rowBuffer.putInt(rid);
            rowBuffer.put(pid.getBytes());
            rowBuffer.put(score);
            rowBuffer.putShort(helpN);
            rowBuffer.putShort(helpD);
            rowBuffer.putShort(reviewLength);
            return rowBuffer.array();
        }


    @Override
    public int compareTo(ReviewMetaData o) {
        return Integer.compare(this.rid, o.rid);
    }
}
