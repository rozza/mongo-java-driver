function initializeJS() {
  jQuery('.selectpicker').selectpicker();
  jQuery('.distropicker').bootstrapToggle();
  jQuery('.distropicker').change(toggleDownload);
  jQuery('.selectpicker').change(toggleDownload);
};

var toggleDownload = function() {
    prefix = $('.distropicker').prop('checked') ? "#maven-" : "#gradle-"
    version = $('.selectpicker').selectpicker().val();
    activeSample = prefix + version;
    $('.maven').hide();
    $('.gradle').hide();
    $(activeSample).show();
};

jQuery(document).ready(function(){
    initializeJS();
    jQuery("body").addClass("hljsCode");
    hljs.initHighlightingOnLoad();
});
