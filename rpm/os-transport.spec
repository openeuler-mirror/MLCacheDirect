Name:           os-transport
Version:        %{version}
Release:        %{release}
Summary:        OS transport layer shared library
License:        MIT
BuildArch:      %{build_arch}
Requires:       glibc >= 2.17

%description
OS transport layer library (libos_transport.so) for data send/recv.

# ========== devel子包：仅包含头文件 ==========
%package devel
Summary:        Header files for os-transport (only header)
Requires:       %{name} = %{version}-%{release}

%description devel
Only header files for os-transport, used for external project compilation.

%prep
# 空（build.sh已处理源码路径）

%build
# 空（build.sh已完成编译）

%install
# 兜底：确保文件复制到BUILDROOT
cp -r %{install_root}/* %{buildroot}/

# ========== 主包文件（运行库） ==========
# 注释移到独立行，避免rpmbuild解析错误
# 完整版本库
# 主版本软链接
# 编译链接用软链接
%files
%defattr(-,root,root)
/usr/lib64/libos_transport.so.%{version}
/usr/lib64/libos_transport.so.%{version_major}
/usr/lib64/libos_transport.so

# ========== devel包文件（仅头文件） ==========
%files devel
%defattr(-,root,root)
/usr/include/os-transport/os_transport.h

# ========== 触发ldconfig更新库缓存 ==========
%post -p /sbin/ldconfig
%postun -p /sbin/ldconfig

%changelog
* Sun Mar 08 2026 OS Dev <dev@example.com> - 1.0.0-1
- Initial RPM package for os-transport
- Support x86_64/aarch64 auto-detection
- Devel package only contains header files
