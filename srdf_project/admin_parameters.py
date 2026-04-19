from django.contrib import admin

# ===================================
# ==== DATA MODELS GROUP          ===
from srdf.models              import  *


# ==================================
# ==== ADMIN MODULES             ===
from srdf_project.admin_srdf            import *




from django.contrib.sites.models import Site

from oauth2_provider.models import *
from oauth2_provider.admin import *

#  --------------------------------------------
#  ---- second admin for system processing  ---
#  --------------------------------------------
from django.contrib.admin import AdminSite
class EventAdminSite(AdminSite):
	site_header = "IdeoLab SRDF Admin"
	site_title  = "IdeoLab SRDF Admin"
	index_title = "IdeoLab SRDF Admin"
	
	
# -----------------------------------------
#  ---         API  admin and Options   ---
parameters_admin_site      = EventAdminSite(name='parameters_admin'       )
parameters_admin_site_2024 = EventAdminSite(name='parameters_admin_2024'  )


# -----------------------------------------
# -----------------------------------------
#  --- SRDF SECTION                     ---
# -----------------------------------------
# -----------------------------------------

parameters_admin_site.register(Node                   , NodeAdmin       )
parameters_admin_site.register(ReplicationService     , ReplicationServiceAdmin          )
parameters_admin_site.register(ReplicationStatus      , ReplicationStatusAdmin          )
parameters_admin_site.register(ActionExecution        , ActionExecutionAdmin          )
parameters_admin_site.register(FailoverPlan           , FailoverPlanAdmin          )
parameters_admin_site.register(AuditEvent             , AuditEventAdmin          )
parameters_admin_site.register(ExecutionLock          , ExecutionLockAdmin          )

parameters_admin_site.register(OutboundChangeEvent, OutboundChangeEventAdmin)
parameters_admin_site.register(TransportBatch, TransportBatchAdmin)
parameters_admin_site.register(InboundChangeEvent, InboundChangeEventAdmin)
parameters_admin_site.register(ReplicationCheckpoint, ReplicationCheckpointAdmin)
parameters_admin_site.register(SQLObjectScopeRule, SQLObjectScopeRuleAdmin)
parameters_admin_site.register(BootstrapPlan, BootstrapPlanAdmin)
parameters_admin_site.register(BootstrapPlanTableSelection, BootstrapPlanTableSelectionAdmin)

parameters_admin_site.register(SourceDatabaseCatalog, SourceDatabaseCatalogAdmin)
parameters_admin_site.register(SourceTableCatalog, SourceTableCatalogAdmin)

parameters_admin_site.register(BootstrapPlanExecution, BootstrapPlanExecutionAdmin)
parameters_admin_site.register(BootstrapPlanExecutionItem, BootstrapPlanExecutionItemAdmin)

parameters_admin_site.register(SRDFOrchestratorRun, SRDFOrchestratorRunAdmin)
parameters_admin_site.register(SRDFServiceRuntimeState, SRDFServiceRuntimeStateAdmin)
parameters_admin_site.register(SRDFServicePhaseExecution, SRDFServicePhaseExecutionAdmin)
parameters_admin_site.register(SRDFServiceExecutionLock, SRDFServiceExecutionLockAdmin)
parameters_admin_site.register(SRDFSettingDefinition, SRDFSettingDefinitionAdmin)
parameters_admin_site.register(SRDFSettingValue, SRDFSettingValueAdmin)

parameters_admin_site.register(SRDFCaptureControlRun, SRDFCaptureControlRunAdmin)
parameters_admin_site.register(SRDFCaptureControlItem, SRDFCaptureControlItemAdmin)
parameters_admin_site.register(SRDFCronExecution, SRDFCronExecutionAdmin)

parameters_admin_site.register(SRDFDedupRun, SRDFDedupRunAdmin)
parameters_admin_site.register(SRDFDedupDecision, SRDFDedupDecisionAdmin)

parameters_admin_site.register(SRDFArchiveExecutionLock, SRDFArchiveExecutionLockAdmin)
parameters_admin_site.register(SRDFArchiveRunItem, SRDFArchiveRunItemAdmin)
parameters_admin_site.register(SRDFArchiveRun, SRDFArchiveRunAdmin)

parameters_admin_site.register(SRDFInstallProfile, SRDFInstallProfileAdmin)
parameters_admin_site.register(SRDFInstallRun, SRDFInstallRunAdmin)


