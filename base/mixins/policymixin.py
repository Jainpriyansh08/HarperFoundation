class PermissionPolicyMixin:

    def check_permissions(self, request):
        try:
            handler = self.action
        except AttributeError:
            handler = None

        if handler and self.permission_classes_per_method and self.permission_classes_per_method.get(handler):
            self.permission_classes = self.permission_classes_per_method.get(handler)
        super().check_permissions(request)


class PermissionPolicyMixinRequestType(PermissionPolicyMixin):

    def check_permissions(self, request):
        try:
            handler = request.method
        except AttributeError:
            handler = None

        if handler and self.permission_classes_per_request_type and self.permission_classes_per_request_type.get(
                handler):
            self.permission_classes = self.permission_classes_per_request_type.get(handler)
        if not hasattr(self, 'permission_classes_per_method'):
            setattr(self, 'permission_classes_per_method', {})
        super().check_permissions(request)
