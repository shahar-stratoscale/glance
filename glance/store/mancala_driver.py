"""Storage backend for Mancala"""
import sys
sys.path.insert(0, '/usr/share/stratostorage/mancala_management_api.egg')
from mancala.management.externalapi import images
from mancala.common import exception as mancala_exception

from glance.common import exception
import glance.openstack.common.log as logging
import glance.store
import glance.store.base
import glance.store.location

LOG = logging.getLogger(__name__)

def entryExit(f):
    def _decorated(self, *args, **kwargs):
        LOG.debug("Enter: %s" % f.__name__)
        ret = f(self, *args, **kwargs)
        LOG.debug("Exit: %s" % f.__name__)
        return ret
    return _decorated


class StoreLocation(glance.store.location.StoreLocation):
    """
    Class describing a mancala URI. This is mancala://vol-id
    """

    def process_specs(self):
        self.image_id = self.specs.get('image-id')

    def get_uri(self):
        return "mancala://%s" % self.image_id

    def parse_uri(self, uri):
        """Parse the Glance URI"""
        prefix = 'mancala://'
        if not uri.startswith(prefix):
            reason = _('URI must start with mancala://')
            LOG.debug(_("Invalid URI: %(uri)s: %(reason)s") % locals())
            raise exception.BadStoreUri(message=reason)
        try:
            ascii_uri = str(uri)
        except UnicodeError:
            reason = _('URI contains non-ascii characters')
            LOG.debug(_("Invalid URI: %(uri)s: %(reason)s") % locals())
            raise exception.BadStoreUri(message=reason)
        pieces = ascii_uri[len(prefix):].split('/')
        self.image_id = str(pieces[0])


class Store(glance.store.base.Store):
    """An implementation of the Mancala backend adapter."""

    EXAMPLE_URL = "mancala://<VOLID>"

    def get_schemes(self):
        return ('mancala',)

    def configure_add(self):
        try:
            self._imageAPI = images.ImageAPI()
        except Exception:
            msg = 'Failed to load mancala images API'
            LOG.exception(msg)
            raise exception.BadStoreConfiguration(store_name="mancala", reason=msg)

    def get(self, location):
        pass

    def get_size(self, location):
        image_id = location.store_location.image_id
        try:
            image = self._imageAPI.get(str(image_id))
        except mancala_exception.VolumeNotFoundError:
            raise exception.NotFound('Image %s not found' % image_id)
        return image['size']

    @entryExit
    def add(self, image_id, image_file, image_size):
        image = self._imageAPI.create(image_file)
        return ('mancala://%s' % image['externalID'], image_size, None, {})

    @entryExit
    def delete(self, location):
        image_id = location.store_location.image_id
        try:
            self._imageAPI.delete(str(image_id))
        except mancala_exception.VolumeNotFoundError:
            raise exception.NotFound('Image %s not found' % image_id)
