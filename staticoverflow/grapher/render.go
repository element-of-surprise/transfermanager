package grapher

import (
	"bytes"
	"fmt"
	"html/template"
	"io"
	"sync"
	//"github.com/element-of-surprise/transfermanager/staticoverflow"
)

var funcMap = template.FuncMap{
	"safeJS": func(s interface{}) template.JS {
		return template.JS(fmt.Sprint(s))
	},
	"barStyle": func(i int) template.HTML {
		color := "w3-black"
		switch i % 4 {
		case 0:
			color = "w3-black"
		case 1:
			color = "w3-red"
		case 2:
			color = "w3-amber"
		case 3:
			color = "w3-teal"
		}

		return template.HTML(fmt.Sprintf(`<div class="w3-bar %s"><div class="w3-bar-item">Collection %d</div></div>`, color, i))
	},
}

const pageText = `
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Awesome go-echarts</title>
    <script src="https://go-echarts.github.io/go-echarts-assets/assets/echarts.min.js"></script>
    <script src="https://go-echarts.github.io/go-echarts-assets/assets/themes/westeros.js"></script>
    <style>
      .w3-bar{width:100%;overflow:hidden}.w3-center .w3-bar{display:inline-block;width:auto}
      .w3-bar .w3-bar-item{padding:8px 16px;float:left;width:auto;border:none;display:block;outline:0}
      .w3-black,.w3-hover-black:hover{color:#fff!important;background-color:#000!important}
      .w3-red,.w3-hover-red:hover{color:#fff!important;background-color:#f44336!important
      .w3-amber,.w3-hover-amber:hover{color:#000!important;background-color:#ffc107!important}
      .w3-teal,.w3-hover-teal:hover{color:#fff!important;background-color:#009688!important}
    </style>
</head>

<body>

{{- range $index, $elem := .Collection }}
	{{ barStyle $index }}

	<table>
		<caption>Args</caption>
		<tr><td>BlockSize</td><td>{{$elem.Args.BlockSize}}</td></tr>
		<tr><td>StaticBlocks</td><td>{{$elem.Args.StaticBlocks}}</td></tr>
		<tr><td>DynamicLimit</td><td>{{$elem.Args.DynamicLimit}}</td></tr>
		<tr><td>Concurrency</td><td>{{$elem.Args.Concurrency}}</td></tr>
	</table>

	{{- range $elem.Graphs }}
		{{ .Data }}
	{{- end }}
{{- end }}

</body>
</html>
`

var pageTmpl = template.Must(template.New("").Funcs(funcMap).Parse(pageText))

// Borrowed from https://blog.cubieserver.de/2020/how-to-render-standalone-html-snippets-with-go-echarts/
const baseText = `
<div class="container">
    <div class="item" id="{{ .ChartID }}" style="width:{{ .Initialization.Width }};height:{{ .Initialization.Height }};"></div>
</div>
<script type="text/javascript">
    "use strict";
    let goecharts_{{ .ChartID | safeJS }} = echarts.init(document.getElementById('{{ .ChartID | safeJS }}'), "{{ .Theme }}");
    let option_{{ .ChartID | safeJS }} = {{ .JSON }};
    goecharts_{{ .ChartID | safeJS }}.setOption(option_{{ .ChartID | safeJS }});
    {{- range .JSFunctions.Fns }}
    {{ . | safeJS }}
    {{- end }}
</script>
`

var baseTmpl = template.Must(template.New("").Funcs(funcMap).Parse(baseText))

var pool = sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}

// RenderPage renders all graphs to a page.
func RenderPage(r Record) ([]byte, error) {
	buff := pool.Get().(*bytes.Buffer)
	defer func() {
		buff.Reset()
		pool.Put(buff)
	}()

	err := pageTmpl.ExecuteTemplate(buff, "", r)
	if err != nil {
		return nil, err
	}
	return buff.Bytes(), nil
}

// renderChart renders a chart is self contained HTML. Implements e-charts.Renderer.
type renderChart struct {
	c      interface{}
	before []func()
}

func (r *renderChart) Render(w io.Writer) error {
	for _, fn := range r.before {
		fn()
	}

	return baseTmpl.ExecuteTemplate(w, "", r.c)
}
