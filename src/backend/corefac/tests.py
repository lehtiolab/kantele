from kantele.tests import BaseTest
from corefac import models as cm
from datasets import models as dm
from django.test import tag

@tag("core")
class PrepOptionProtocol(BaseTest):
    def test_doi_or_hash(self):
        param = dm.SampleprepParameter.objects.create(title = "DOI Testing")
        paramopt = dm.SampleprepParameterOption.objects.create(
            param_id=param.pk,
            value='1'
        )
        version = 1
        doi_obj = cm.PrepOptionProtocol.objects.create(paramopt_id=paramopt.pk,doi = 'doi.org/10.1234/abc.456',version = version)
        self.assertEqual(doi_obj.doi,'doi.org/10.1234/abc.456')
        hash_obj = cm.PrepOptionProtocol.objects.create(paramopt_id=paramopt.pk,doi =  'abcd1234',version = version)
        self.assertEqual(hash_obj.doi,'protocols.io/view/abcd1234')