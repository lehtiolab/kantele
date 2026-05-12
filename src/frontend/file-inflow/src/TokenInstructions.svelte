<script>
export let fileType;

const isWindows = navigator.appVersion.toLowerCase().indexOf('win') != -1 ? 1 : 0;

</script>


<h5 class="subtitle is-5">Instructions for upload of big files with token</h5>
<div class="content">
  You can upload your own larger {fileType} files to storage.
  If not done previously, download the latest upload client at <a href="https://github.com/lehtiolab/kantele/releases">the Kantele repo github</a> and run it.
  Paste the upload token in the box. The same token can be used for multiple file uploads
  for one analysis, or in case of e.g. library files, of the same file type.
 
  Do not upload folders, if that is desired (e.g. enormous amount of images or something), please
  zip them first.
</div>
