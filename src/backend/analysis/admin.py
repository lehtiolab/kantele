from django.contrib import admin

from analysis.models import (NextflowWfVersionParamset, NextflowWorkflowRepo, UserWorkflow, ParameterSet,
                             Param, FileParam, PsetParam, PsetFileParam, PsetMultiFileParam,
                             ParamOption, Proteowizard, PsetComponent, WfOutput, OutputFieldName,
                             PipelineVersionOutput, NfConfigFile)


admin.site.register(NextflowWfVersionParamset)
admin.site.register(NextflowWorkflowRepo)
admin.site.register(UserWorkflow)
admin.site.register(PsetComponent)
admin.site.register(ParameterSet)
admin.site.register(PsetParam)
admin.site.register(PsetFileParam)
admin.site.register(PsetMultiFileParam)
admin.site.register(Param)
admin.site.register(ParamOption)
admin.site.register(FileParam)
admin.site.register(Proteowizard)
admin.site.register(NfConfigFile)

admin.site.register(WfOutput)
admin.site.register(OutputFieldName)
admin.site.register(PipelineVersionOutput)
