package ru.dsi.bgbilling.modules.inet.accounting.quota;

import org.apache.log4j.Logger;
import ru.bitel.bgbilling.kernel.container.managed.ServerContext;
import ru.bitel.bgbilling.kernel.event.EventProcessor;
import ru.bitel.common.worker.WorkerTask;
import ru.dsi.bgbilling.modules.inet.accounting.quota.event.QuotaProfileChangedEvent;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Процесс-обработчик
 */
public class QuotaCheckerWorker extends WorkerTask<ServerContext> {
    private static final Logger logger = Logger.getLogger(QuotaCheckerWorker.class);

    //servId -> nodeId -> QuotaHolder
    private final ConcurrentHashMap<Integer, ConcurrentHashMap<Long, QuotaHolder>> quotaMap;
    private final int mid;

    public QuotaCheckerWorker(int mid, ConcurrentHashMap<Integer, ConcurrentHashMap<Long, QuotaHolder>> quotaMap) {
        this.quotaMap = quotaMap;
        this.mid = mid;
    }

    @Override
    protected void runImpl() throws Exception {
        QuotaProfile quota;
        String newProfileName;
        //Перебираем все квоты
        int cnt=0;
        int servDataRemovedCnt = 0;
        int nodeDataRemovedCnt = 0;
        int cntChanged=0;
        long start = System.currentTimeMillis();
        logger.debug("run QuotaCheckerWorker");
        for (ConcurrentHashMap.Entry<Integer, ConcurrentHashMap<Long, QuotaHolder>> servQuotaEntry : this.quotaMap.entrySet()) {
            if(servQuotaEntry.getValue().isEmpty()){
                //Для сервиса нет данных о квоте, удаляем элемент
                this.quotaMap.remove(servQuotaEntry.getKey());
                servDataRemovedCnt++;
            }else{
                for (Map.Entry<Long, QuotaHolder> nodeQuotaEntry : servQuotaEntry.getValue().entrySet()){
                    quota = nodeQuotaEntry.getValue().quota;
                    if(quota!=null){
                        cnt++;
                        //penalty expired?
                        if(quota.penaltyExpiredTime>0 && quota.penaltyExpiredTime<=System.currentTimeMillis() && quota.getTotalAmount()<quota.quotaSize){//penaltyTime прошло, можно поднимать профиль, если size не превышен
                            newProfileName = quota.getUpProfileName();
                            if(newProfileName!=null){
                                cntChanged++;
                                //меняем квоту!
                                //Положим период "протухания" при переключении на новую квоту = периоду старой квоты
                                synchronized (nodeQuotaEntry.getValue()){
                                    nodeQuotaEntry.getValue().expireTime = System.currentTimeMillis()+quota.penaltyPeriod;
                                    nodeQuotaEntry.getValue().name = newProfileName;
                                    nodeQuotaEntry.getValue().quota = null;
                                }
                                //Кидаем событие смены квоты
                                EventProcessor.getInstance().publish(new QuotaProfileChangedEvent(this.mid, newProfileName, servQuotaEntry.getKey(), nodeQuotaEntry.getKey()));
                                logger.info("penalty period expired for servId="+servQuotaEntry.getKey()+
                                ": "+quota.name+" -> "+newProfileName+" ("+quota.getTotalAmount()+" bytes consumed)");
                                //TODO - кидаем событие на тарификацию!
                            }
                        }
                    }
                    //quota object expired?
                    if(nodeQuotaEntry.getValue().expireTime>0 && System.currentTimeMillis()>nodeQuotaEntry.getValue().expireTime){
                        servQuotaEntry.getValue().remove(nodeQuotaEntry.getKey());
                        logger.debug("quotaProfile "+nodeQuotaEntry.getValue().name+" expired for servId="+servQuotaEntry.getKey());
                        nodeDataRemovedCnt++;
                    }
                }
            }
        }
        logger.debug("QuotaCheckerWorker: "+cntChanged+"/"+cnt+" changed, "+servDataRemovedCnt+"(serv) + "+nodeDataRemovedCnt+"(node) stale data removed ["+(System.currentTimeMillis()-start)+" msec]");
    }
}
