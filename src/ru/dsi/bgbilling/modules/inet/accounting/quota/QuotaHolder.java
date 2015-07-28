package ru.dsi.bgbilling.modules.inet.accounting.quota;

/**
 * TODO
 */
public class QuotaHolder{
    public volatile String name;
    public volatile QuotaProfile quota;
    /**
     * true, если есть не сохранённые на диск изменения
     */
    public volatile boolean modified;
    /**
     * Момент времени, после которого при quota==null будем считать, что переключение на новую квоту сфейлилось
     * и можно удалять объект QuotaHolder из мэпы.
     */
    public volatile long expireTime;

    public QuotaHolder(QuotaProfile quota){
        this.quota = quota;
        this.name = quota.name;
        this.expireTime=System.currentTimeMillis()+quota.expirePeriod;
        this.modified=true;
    }

    public QuotaHolder(String name){
        this.quota = null;
        this.name = name;
        this.expireTime=-1;
        this.modified=true;
    }
}