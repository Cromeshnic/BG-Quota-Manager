package ru.dsi.bgbilling.modules.inet.accounting.quota;

import org.apache.log4j.Logger;
import ru.bitel.bgbilling.kernel.container.managed.ServerContext;
import ru.bitel.bgbilling.server.util.ServerUtils;
import ru.bitel.bgbilling.server.util.Setup;
import ru.bitel.common.worker.WorkerTask;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Процесс, который периодически сохраняет runtime-данные о квотах клиентов в базу для предоставления статистики и
 * возможности перезагрузки Access/Accounting серверов без потери данных
 */
public class QuotaFlushingWorker extends WorkerTask<ServerContext> {
    private static final Logger logger = Logger.getLogger(QuotaFlushingWorker.class);

    //servId -> nodeId -> QuotaHolder
    private final ConcurrentHashMap<Integer, ConcurrentHashMap<Long, QuotaHolder>> quotaMap;
    private final int mid;
    private final Setup setup;

    public QuotaFlushingWorker(int mid, Setup setup, ConcurrentHashMap<Integer, ConcurrentHashMap<Long, QuotaHolder>> quotaMap) {
        this.mid = mid;
        this.setup = setup;
        this.quotaMap = quotaMap;
    }

    @Override
    protected void runImpl() throws Exception {
        Connection con = this.setup.getDBConnectionFromPool();
        QuotaManager quotaManager = new QuotaManager(con, this.mid);
        try{
            //Перебираем все квоты
            int cnt = 0;
            int cntFlushed = 0;
            long start = System.currentTimeMillis();
            logger.debug("run QuotaFlushingWorker");
            for (ConcurrentHashMap.Entry<Integer, ConcurrentHashMap<Long, QuotaHolder>> servQuotaEntry : this.quotaMap.entrySet()) {
                for (Map.Entry<Long, QuotaHolder> nodeQuotaEntry : servQuotaEntry.getValue().entrySet()){
                    cnt++;
                    if(!nodeQuotaEntry.getValue().modified){
                        continue;
                    }
                    try{
                        quotaManager.save(servQuotaEntry.getKey(), nodeQuotaEntry.getKey(), nodeQuotaEntry.getValue());
                        cntFlushed++;
                    }catch (SQLException e){
                        logger.error(e.getMessage(), e);
                    }
                }
            }
            logger.debug(cntFlushed+"/"+cnt+" flushed ["+(System.currentTimeMillis()-start)+" msec]");
            logger.debug("Cleaning up...");
            try{
                quotaManager.cleanUp();
            }catch (SQLException e){
                logger.error(e.getMessage(), e);
            }
            logger.debug("...finished");
        }finally {
            quotaManager.recycle();
            ServerUtils.closeConnection(con);
        }
    }
}
