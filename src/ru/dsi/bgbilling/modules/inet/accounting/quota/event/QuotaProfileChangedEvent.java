package ru.dsi.bgbilling.modules.inet.accounting.quota.event;

import ru.bitel.bgbilling.kernel.event.Event;
import ru.bitel.bgbilling.kernel.event.EventType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Событие смены квот
 */
@EventType(noLocal=true)
@XmlRootElement
public class QuotaProfileChangedEvent extends Event {

    @XmlAttribute
    private final String quotaName;
    @XmlAttribute
    private final Integer servId;
    @XmlAttribute
    private final Long nodeId;

    protected QuotaProfileChangedEvent(){
        this.quotaName = null;
        this.servId=-1;
        this.nodeId=-1L;
    }

    public QuotaProfileChangedEvent(int moduleId, String quotaName, Integer servId, Long nodeId) {
        super(moduleId, CONTRACT_GLOBAL, 0);
        this.quotaName = quotaName;
        this.servId = servId;
        this.nodeId = nodeId;
    }

    public String getQuotaName() {
        return quotaName;
    }

    public Integer getServId() {
        return servId;
    }

    public Long getNodeId() {
        return nodeId;
    }

    protected void toString(StringBuilder sb)
    {
        sb.append("; servId: ");
        sb.append(this.servId);
        sb.append("; nodeId: ");
        sb.append(this.nodeId);
        sb.append("; quotaName: ");
        sb.append(this.quotaName);

    }
}
