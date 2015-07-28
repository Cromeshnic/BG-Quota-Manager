package ru.dsi.bgbilling.modules.inet.accounting.quota;

import org.apache.log4j.Logger;
import ru.bitel.common.Preferences;
import ru.bitel.common.Utils;
import ru.bitel.common.worker.Recyclable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * MySQL tables:
 *
 * CREATE TABLE `custom_inet_quota_[MID]` (
 * `servId` int(11) NOT NULL,
 * `nodeId` bigint(20) NOT NULL,
 * `name` varchar(64) NOT NULL,
 * `expireTime` bigint(20) NOT NULL,
 * `penaltyExpiredTime` bigint(20) NOT NULL,
 * `params` varchar(256) DEFAULT NULL,
 * PRIMARY KEY (`servId`,`nodeId`)
 * )
 *
 * CREATE TABLE `custom_inet_quota_slices_[MID]` (
 * `servId` int(11) NOT NULL,
 * `nodeId` bigint(20) NOT NULL,
 * `amount` bigint(20) NOT NULL,
 * `endTime` bigint(20) NOT NULL,
 * PRIMARY KEY (`servId`,`nodeId`, `endTime`)
 * )
 *
 *
 */
@SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
public class QuotaManager implements Recyclable{
    private static final Logger logger = Logger.getLogger(QuotaManager.class);

    private Connection con;

    private final String tableName;
    private final String slicesTableName;

    private PreparedStatement psUpdate;
    private PreparedStatement psUpdateSlices;
    private PreparedStatement psDeleteSilces;

    //private PreparedStatement psSelectAll;
    //private PreparedStatement psSelectSlices;

    public QuotaManager(Connection con, int mid){
        this.con = con;
        this.tableName = "custom_inet_quota_"+mid;
        /* servId, nodeId, name, expireTime, penaltyExpiredTime */
        this.slicesTableName = "custom_inet_quota_slices_"+mid;
        this.psUpdate = null;
        this.psUpdateSlices = null;
        this.psDeleteSilces = null;
        //this.psSelectSlices = null;
    }

    /**
     *
     * @param quotaMap
     * @param loadSlices
     * @throws SQLException
     */
    public void loadAll(ConcurrentHashMap<Integer, ConcurrentHashMap<Long, QuotaHolder>> quotaMap, boolean loadSlices) throws SQLException {

        PreparedStatement psSelectSlices = null;
        if(loadSlices){
            psSelectSlices = this.con.prepareStatement("select amount, endTime from "+this.slicesTableName+" where servId=? and nodeId=?");
        }

        PreparedStatement psSelectAll =
                this.con.prepareStatement("select * from "+this.tableName+" where expireTime>=UNIX_TIMESTAMP()*1000");
        ResultSet rs = psSelectAll.executeQuery();
        QuotaHolder quotaHolder;
        int servId;
        long nodeId;
        long penaltyExpiredTime;
        ResultSet rsSlices;
        List<Slice> slices;
        ConcurrentHashMap<Long, QuotaHolder> servQuotaMap;
        while(rs.next()){
            servId = rs.getInt("servId");
            nodeId = rs.getLong("nodeId");
            penaltyExpiredTime = rs.getLong("penaltyExpiredTime");
            quotaHolder = this.getFromRs(rs);
            //Есть ли данные трафиков для квоты?
            if(loadSlices){
                psSelectSlices.setInt(1, servId);
                psSelectSlices.setLong(2, nodeId);
                rsSlices = psSelectSlices.executeQuery();
                slices = new ArrayList<Slice>();
                while(rsSlices.next()){
                    slices.add(new Slice(rsSlices.getLong(1), rsSlices.getLong(2)));
                }
                rsSlices.close();
                if(slices.size()>0){
                    quotaHolder.quota = new QuotaProfile(new Preferences(rs.getString("params"), "\n"), penaltyExpiredTime, slices);
                    if(!Utils.isEmptyString(quotaHolder.quota.getErrorString())){
                        logger.error("Error loading quota from db (servId="+servId+", nodeId="+nodeId+", quota="+quotaHolder.name+"): "+quotaHolder.quota.getErrorString());
                    }
                }
            }
            servQuotaMap = quotaMap.get(servId);
            if(servQuotaMap==null){
                servQuotaMap = new ConcurrentHashMap<Long, QuotaHolder>();
                quotaMap.putIfAbsent(servId, servQuotaMap);
                servQuotaMap = quotaMap.get(servId);
            }
            servQuotaMap.put(nodeId, quotaHolder);
            logger.debug("quota data loaded for servId="+servId+", nodeId="+nodeId+": "+quotaHolder.name+(quotaHolder.quota==null? "" : " ("+quotaHolder.quota.getTotalAmount()+"/"+quotaHolder.quota.quotaSize+")"));
        }
        rs.close();
        psSelectAll.close();
        if(psSelectSlices!=null){
            psSelectSlices.close();
        }
        /*if(this.psSelectAll == null || this.psSelectAll.isClosed()){
            this.psSelectAll = this.con.prepareStatement("select * from "+this.tableName+" where expireTime>=UNIX_TIMESTAMP()*1000");
        }*/
    }

    /**
     * Прибираемся - удаляем устаревщие квоты из базы
     */
    public void cleanUp() throws SQLException {
        PreparedStatement ps = this.con.prepareStatement("delete from "+this.tableName+" where expireTime<=UNIX_TIMESTAMP()*1000");
        ps.executeUpdate();
        ps.close();
        ps = this.con.prepareStatement("delete from "+this.slicesTableName+" where not exists (select * from "+this.tableName+" q where q.servId="+this.slicesTableName+".servId and q.nodeId="+this.slicesTableName+".nodeId) ");
        ps.executeUpdate();
        ps.close();
    }

    private QuotaHolder getFromRs(ResultSet rs) throws SQLException {
        QuotaHolder result = new QuotaHolder(rs.getString("name"));
        result.expireTime = rs.getLong("expireTime");
        return result;
    }

    public void save(int servId, long nodeId, QuotaHolder quotaHolder) throws SQLException {
        if(quotaHolder==null){return;}

        String name;
        long expireTime;
        QuotaProfile quota;

        synchronized (quotaHolder){
            name = quotaHolder.name;
            expireTime = quotaHolder.expireTime;
            quota = quotaHolder.quota;
            quotaHolder.modified = false;
        }

        //Удаляем старые данные о слайсах
        if(this.psDeleteSilces==null || this.psDeleteSilces.isClosed()){
            this.psDeleteSilces = this.con.prepareStatement("delete from "+this.slicesTableName+" where servId=? and nodeId=?");
        }
        this.psDeleteSilces.setInt(1, servId);
        this.psDeleteSilces.setLong(2, nodeId);
        this.psDeleteSilces.executeUpdate();

        if(this.psUpdate==null || this.psUpdate.isClosed()){
            this.psUpdate = this.con.prepareStatement("insert into "+tableName+" (servId, nodeId, name, expireTime, penaltyExpiredTime, params) " +
                    "values (?, ?, ?, ?, ?, ?) on duplicate key update " +
                    "name = ?, expireTime = ?, penaltyExpiredTime = ?, params = ?");
        }
        this.psUpdate.setInt(1, servId);
        this.psUpdate.setLong(2, nodeId);
        this.psUpdate.setString(3, name);
        this.psUpdate.setLong(4, expireTime);
        this.psUpdate.setLong(5, ( quota==null ? -1 : quota.penaltyExpiredTime ));
        this.psUpdate.setString(6, (quota == null ? "" : quota.getParams().toString()));
        this.psUpdate.setString(7, name);
        this.psUpdate.setLong(8, expireTime);
        this.psUpdate.setLong(9, ( quota==null ? -1 : quota.penaltyExpiredTime ));
        this.psUpdate.setString(10, (quota==null ? "" : quota.getParams().toString() ));
        this.psUpdate.executeUpdate();
        if(quota!=null){//Сохраняем данные по слайсам
            if(this.psUpdateSlices==null || this.psUpdateSlices.isClosed()){
                this.psUpdateSlices = this.con.prepareStatement("insert into "+slicesTableName+" (servId, nodeId, amount, endTime) values(?,?,?,?)");
            }
            this.psUpdateSlices.setInt(1, servId);
            this.psUpdateSlices.setLong(2, nodeId);
            for (Slice slice : quota.getSlices()) {
                this.psUpdateSlices.setLong(3, slice.amount.get());
                this.psUpdateSlices.setLong(4, slice.endTime);
                this.psUpdateSlices.executeUpdate();
            }
        }
    }

    @Override
    public void recycle() {
        if(this.psUpdate!=null){
            try {
                this.psUpdate.close();
            } catch (SQLException e) {
            }
        }
        if(this.psUpdateSlices!=null){
            try {
                this.psUpdateSlices.close();
            } catch (SQLException e) {
            }
        }
        if(this.psDeleteSilces!=null){
            try {
                this.psDeleteSilces.close();
            } catch (SQLException e) {
            }
        }

    }
}
