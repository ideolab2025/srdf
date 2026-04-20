#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
This file was generated with the customdashboard management command, it
contains the two classes for the main dashboard and app index dashboard.
You can customize these classes as you want.

https://django-admin-tools.readthedocs.io/en/latest/customization.html#customizing-the-dashboards
https://django-admin-tools.readthedocs.io/en/latest/customization.html#customizing-the-dashboards
"""

from django.utils.translation import gettext_lazy as _
try:
    from django.urls import reverse
except ImportError:
    from django.core.urlresolvers import reverse

from django.conf                  import settings

#======================================
#===== ADMIN TOOLS DEFINITIONS      ===
#======================================
from admin_tools.dashboard         import modules, Dashboard, AppIndexDashboard
from admin_tools.utils             import get_admin_site_name
from ideolab_admin_tools.dashboard import CountedAppList, VisibilityToggleModule


class CustomIndexDashboard(Dashboard):
    """
    Custom index dashboard for Kocliko Admin API.
    """
    columns =  2
    
    def init_with_context(self, context):
        site_name = get_admin_site_name(context)
        # append a link list module for "quick links"
        
        
        self.children.append(modules.LinkList(
            _('IDEOLAB ADMIN SRDF'),
            layout='inline',
            draggable=True,
            deletable=True,
            collapsible=True,
            children=[
                [_('SRDF'), '/admin'],
                [_('SYST'),    ' /admin-system'],
                [_('HOME'),    '/'],
                [_('PSW'),
                 reverse('%s:password_change' % site_name)],
                [_('EXIT'), reverse('%s:logout' % site_name)],
            ]
        ))
        
        self.children.append(VisibilityToggleModule())       

        self.children.append(CountedAppList(
            _('----  SRDF TOPOLOGY / INSTALL / SETTINGS  ----')   ,
            models=( 'srdf.models.Node'                           ,
                     "srdf.models.ReplicationService"             , 
                     "srdf.models.SRDFInstallProfile"             ,
                     "srdf.models.SRDFInstallRun"                 ,
                     "srdf.models.SRDFSettingDefinition"          ,
                     "srdf.models.SRDFSettingValue"               ,
                     ),
        ))
        
        self.children.append(CountedAppList(
            _('----  SRDF RUNTIME / ORCHESTRATOR / LOCKS  ----')       ,
            models=( 'srdf.models.SRDFOrchestratorRun'                 ,
                     "srdf.models.SRDFServiceRuntimeState"             , 
                     "srdf.models.SRDFServicePhaseExecution"           ,
                     "srdf.models.ExecutionLock"                       ,
                     "srdf.models.SRDFSettingDefinition"               ,
                     "srdf.models.SRDFServiceExecutionLock"            ,
                     "srdf.models.SRDFCronExecution"                   ,
                     ),
        ))        

        self.children.append(CountedAppList(
            _('----  SRDF CAPTURE / TRANSPORT / APPLY  ----')          ,
            models=( 'srdf.models.OutboundChangeEvent'                 ,
                     "srdf.models.TransportBatch"                      , 
                     "srdf.models.InboundChangeEvent"                  , 
                     "srdf.models.ReplicationCheckpoint"               ,
                     "srdf.models.ReplicationStatus"                   ,
                     "srdf.models.SRDFCaptureControlRun"               ,
                     "srdf.models.SRDFCaptureControlItem"              ,
                     "srdf.models.SRDFDedupRun"                        ,
                     "srdf.models.SRDFDedupDecision"                   ,
                     ),
        ))        


        self.children.append(CountedAppList(
            _('----  SRDF BOOTSTRAP / CATALOG / SCOPE  ----')          ,
            models=( 'srdf.models.SQLObjectScopeRule'                  ,
                     "srdf.models.BootstrapPlan"                       , 
                     "srdf.models.SourceDatabaseCatalog"               , 
                     "srdf.models.SourceTableCatalog"                  ,
                     "srdf.models.BootstrapPlanTableSelection"         ,
                     "srdf.models.BootstrapPlanExecution"              ,
                     "srdf.models.BootstrapPlanExecutionItem"          ,
                     ),
        ))        
 

        self.children.append(CountedAppList(
            _('----  SRDF FAILOVER / AUDIT / ARCHIVE  ----')          ,
            models=( 'srdf.models.FailoverPlan'                       ,
                     "srdf.models.ActionExecution"                    , 
                     "srdf.models.AuditEvent"                         , 
                     "srdf.models.SRDFArchiveExecutionLock"           ,
                     "srdf.models.SRDFArchiveRun"                     ,
                     "srdf.models.SRDFArchiveRunItem"                 ,
                     ),
        ))   
        
        
        self.children.append(CountedAppList(
            _('---- IDEOLAB AUDIT & SYSTEM LOGS ----'),
            models=(
                "ideolab_admin_tools.models.AdminAuditEvent",
            ),
        ))        
 





        # ---------------------------------------
        # --- append a recent actions module  ---
        # ---------------------------------------
        if settings.ENABLE_DJANGO_NEWS:
            self.children.append(modules.RecentActions(_('Recent Actions'), 5))

            # append a feed module
            self.children.append(modules.Feed(
                _('Latest Django News'),
                feed_url='http://www.djangoproject.com/rss/weblog/',
                limit=5
            ))

        # ------------------------------------------------------
        # --- append another link list module for "support". ---
        # ------------------------------------------------------
        if settings.ENABLE_DJANGO_NEWS:
            self.children.append(modules.LinkList(
                _('Support'),
                children=[
                    {
                        'title': _('Django documentation'),
                        'url': 'http://docs.djangoproject.com/',
                        'external': True,
                    },
                    {
                        'title': _('Django "django-users" mailing list'),
                        'url': 'http://groups.google.com/group/django-users',
                        'external': True,
                    },
                    {
                        'title': _('Django irc channel'),
                        'url': 'irc://irc.freenode.net/django',
                        'external': True,
                    },
                ]
            ))


class CustomAppIndexDashboard(AppIndexDashboard):
    """
    Custom app index dashboard for easymedstat.
    """

    # we disable title because its redundant with the model list module
    title = 'Graphic API menu'

    def __init__(self, *args, **kwargs):
        AppIndexDashboard.__init__(self, *args, **kwargs)

        # append a model list module and a recent actions module
        self.children += [
            modules.ModelList(self.app_title, self.models),
            modules.RecentActions(
                _('Recent Actions'),
                include_list=self.get_app_content_types(),
                limit=5
            )
        ]

    def init_with_context(self, context):
        """
        Use this method if you need to access the request context.
        """
        return super(CustomAppIndexDashboard, self).init_with_context(context)
