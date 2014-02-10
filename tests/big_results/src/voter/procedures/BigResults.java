package voter.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

public class BigResults extends VoltProcedure
{
    static final String name = "ZZZ%05d";
    static final int base = 1000000;
    static final int count = 100000;

    public VoltTable[] run() {
        VoltTable[] results = new VoltTable[1];
        results[0] = new VoltTable(
            new VoltTable.ColumnInfo("contestant_name", VoltType.STRING),
            new VoltTable.ColumnInfo("contestant_number", VoltType.INTEGER),
            new VoltTable.ColumnInfo("num_votes", VoltType.BIGINT));
        for (int i = 0; i < count; ++i) {
            results[0].addRow(String.format(name, i), base+i, base+i);
        }
        return results;
    }
}
