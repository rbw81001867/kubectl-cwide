package get

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kubectl-cwide/pkg/utils"
	"golang.org/x/sync/errgroup"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/cli-runtime/pkg/printers"
	"k8s.io/cli-runtime/pkg/resource"
	"k8s.io/client-go/discovery"
	"k8s.io/kubectl/pkg/scheme"
)

type resourceResult struct {
	Kind       string
	Group      string
	Version    string
	PluralName string
	Infos      []*resource.Info
}

func (o *GetOptions) listAllResources() error {
	discoveryClient, err := o.factory.ToDiscoveryClient()
	if err != nil {
		return fmt.Errorf("failed to create discovery client: %w", err)
	}

	_, resourceLists, err := discoveryClient.ServerGroupsAndResources()
	if err != nil {
		if !discovery.IsGroupDiscoveryFailedError(err) {
			return fmt.Errorf("failed to discover server resources: %w", err)
		}
		fmt.Fprintf(o.ErrOut, "Warning: some API groups could not be discovered: %v\n", err)
	}

	type resourceTarget struct {
		PluralName string
		Kind       string
		Group      string
		Version    string
	}

	var targets []resourceTarget
	for _, resourceList := range resourceLists {
		gv := resourceList.GroupVersion
		var group, version string
		parts := strings.SplitN(gv, "/", 2)
		if len(parts) == 2 {
			group = parts[0]
			version = parts[1]
		} else {
			version = parts[0]
		}

		for _, r := range resourceList.APIResources {
			if strings.Contains(r.Name, "/") {
				continue
			}
			if !r.Namespaced && !o.AllNamespaces {
				continue
			}
			if !hasListVerb(r.Verbs) {
				continue
			}

			gvk := schema.GroupVersionKind{Group: group, Version: version, Kind: r.Kind}
			templateDir := utils.GenerateDirNameByGVK(gvk)
			if !templateExists(o.TemplateRootPath, templateDir, o.Template) {
				continue
			}

			targets = append(targets, resourceTarget{
				PluralName: r.Name,
				Kind:       r.Kind,
				Group:      group,
				Version:    version,
			})
		}
	}

	if len(targets) == 0 {
		fmt.Fprintf(o.ErrOut, "No resources with templates found. Run 'init' first.\n")
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(o.Timeout)*time.Second)
	defer cancel()

	var mu sync.Mutex
	var results []resourceResult

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(o.Concurrency)

	for _, t := range targets {
		t := t
		g.Go(func() error {
			if ctx.Err() != nil {
				return nil
			}

			var resourceArg string
			if t.Group != "" {
				resourceArg = t.PluralName + "." + t.Group
			} else {
				resourceArg = t.PluralName
			}

			r := o.factory.NewBuilder().
				Unstructured().
				NamespaceParam(o.Namespace).
				AllNamespaces(o.AllNamespaces).
				LabelSelectorParam(o.LabelSelector).
				FieldSelectorParam(o.FieldSelector).
				RequestChunksOf(o.ChunkSize).
				ResourceTypeOrNameArgs(true, resourceArg).
				ContinueOnError().
				Flatten().
				Do()

			if err := r.Err(); err != nil {
				fmt.Fprintf(o.ErrOut, "Warning: failed to list %s: %v\n", resourceArg, err)
				return nil
			}

			infos, err := r.Infos()
			if err != nil {
				fmt.Fprintf(o.ErrOut, "Warning: failed to list %s: %v\n", resourceArg, err)
				return nil
			}

			if len(infos) == 0 {
				return nil
			}

			mu.Lock()
			results = append(results, resourceResult{
				Kind:       t.Kind,
				Group:      t.Group,
				Version:    t.Version,
				PluralName: t.PluralName,
				Infos:      infos,
			})
			mu.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("error listing resources: %w", err)
	}

	if ctx.Err() == context.DeadlineExceeded {
		fmt.Fprintf(o.ErrOut, "Warning: timeout reached (%ds), some resources may not be listed\n", o.Timeout)
	}

	if len(results) == 0 {
		fmt.Fprintf(o.ErrOut, "No resources found in %s namespace.\n", o.Namespace)
		return nil
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].PluralName < results[j].PluralName
	})

	decoder := scheme.Codecs.UniversalDecoder(scheme.Scheme.PrioritizedVersionsAllGroups()...)
	restConfig, err := o.factory.ToRESTConfig()
	if err != nil {
		return fmt.Errorf("failed to get REST config: %w", err)
	}

	for i, res := range results {
		gvk := schema.GroupVersionKind{Group: res.Group, Version: res.Version, Kind: res.Kind}
		templateDir := utils.GenerateDirNameByGVK(gvk)

		printer, err := resolveTemplatePrinter(o.TemplateRootPath, templateDir, o.Template, decoder, restConfig)
		if err != nil {
			fmt.Fprintf(o.ErrOut, "Warning: failed to load template for %s: %v\n", res.PluralName, err)
			continue
		}

		if o.EnableCustomTable {
			printer.WithCustomTable()
		}
		printer.NoHeaders = o.NoHeaders

		if i > 0 {
			fmt.Fprintln(o.Out)
		}
		apiVersion := res.Version
		if res.Group != "" {
			apiVersion = res.Group + "/" + res.Version
		}
		fmt.Fprintf(o.Out, "=== %s (%s) ===\n", res.Kind, apiVersion)

		w := printers.GetNewTabWriter(o.Out)
		for _, info := range res.Infos {
			if err := printer.PrintObj(info.Object, w); err != nil {
				fmt.Fprintf(o.ErrOut, "Warning: failed to print %s/%s: %v\n", res.PluralName, info.Name, err)
			}
		}
		if printer.CustomTable != nil {
			printer.CustomTable.Render()
		} else {
			w.Flush()
		}
	}

	return nil
}

func templateExists(rootPath, templateDir, templateName string) bool {
	dir := filepath.Join(rootPath, templateDir)
	yamlPath := filepath.Join(dir, templateName+".yaml")
	if _, err := os.Stat(yamlPath); err == nil {
		return true
	}
	tplPath := filepath.Join(dir, templateName+".tpl")
	if _, err := os.Stat(tplPath); err == nil {
		return true
	}
	return false
}

func hasListVerb(verbs metav1.Verbs) bool {
	for _, v := range verbs {
		if v == "list" {
			return true
		}
	}
	return false
}
