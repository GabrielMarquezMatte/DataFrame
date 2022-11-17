configure:
	cmake . --preset="win64-msvc-vcpkg"
build:
	cmake --build . --preset="win64-msvc-vcpkg"